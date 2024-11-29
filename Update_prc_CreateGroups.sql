ALTER PROCEDURE prc_CreateGroups
    @partitionId                    INT,
    @scopeId                        UNIQUEIDENTIFIER,
    @errorOnDuplicate               BIT,
    @groups                         typ_GroupTable3 READONLY,
    @eventAuthor                    UNIQUEIDENTIFIER,
    @addActiveScopeMembership       BIT = 1
AS
BEGIN
    SET NOCOUNT ON
    SET XACT_ABORT ON

    DECLARE @status                         INT
    DECLARE @tfError                        NVARCHAR(255)
    DECLARE @internalScopeId                INT
    DECLARE @sequenceId                     BIGINT
    DECLARE @scopePath                      VARBINARY(400)
    DECLARE @rootGroupId                    UNIQUEIDENTIFIER
    DECLARE @errorMessage                   NVARCHAR(2048)
    DECLARE @conflictingGroup               NVARCHAR(256)
    DECLARE @conflictingScope               NVARCHAR(256)
    DECLARE @ids                            typ_GuidTable
    DECLARE @updatedGroups                  typ_GroupTable3
    DECLARE @updatedGroupMemberships        typ_GroupMembershipTable2

    DECLARE @procedureName SYSNAME =  @@SERVERNAME + '.' + DB_NAME() + '.' + OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID)

    BEGIN TRAN

    -- Resolve the scope
    SELECT      @internalScopeId = InternalScopeId,
                @scopePath = ParentPath + CONVERT(BINARY(4), InternalScopeId),
                @conflictingScope = Name
    FROM        tbl_GroupScope WITH (REPEATABLEREAD)
    WHERE       PartitionId = @partitionId
                AND ScopeId = @scopeId
                AND Active =1
    OPTION (OPTIMIZE FOR (@partitionId UNKNOWN))

    IF (@@ROWCOUNT = 0)
    BEGIN
        ROLLBACK TRAN

        DECLARE @msgData NVARCHAR(256) = CONVERT(NVARCHAR(256), @scopeId)
        SET @tfError = dbo.func_GetMessage(400025); RAISERROR(@tfError, 16, -1, @procedureName, @msgData)
        RETURN 400025
    END

    -- Check for insensitive duplicates
    IF(@errorOnDuplicate = 1)
    BEGIN
        DECLARE @collation NVARCHAR(2048)
        -- ICM 548980117. Customer is using SQL_Latin1_General_Pref_CP850_CI_AS. SQL_Latin1_General_Pref_CP850_CI_AS does not exist. Will use SQL_Latin1_General_CP1_CI_AI instead.
        -- SELECT @collation = REPLACE(REPLACE(REPLACE(collation_name, '_AS', '_AI'), '_CS', '_CI'), '_KS', '_KI') FROM sys.columns WHERE object_id = OBJECT_ID('tbl_Group') AND name = 'DisplayName';
        SET @collation = 'SQL_Latin1_General_CP1_CI_AI'
        DECLARE @duplicateDisplayName VARCHAR(256);
        DECLARE @findDuplicateParametersDefinition NVARCHAR(256) = '@groups typ_GroupTable3 READONLY, @internalScopeId INT, @partitionId INT, @duplicateDisplayNameOUT NVARCHAR(256) OUTPUT';
        DECLARE @findDuplicateSQL NVARCHAR(MAX) = '
            SELECT  TOP 1 @duplicateDisplayNameOUT = g.DisplayName
            FROM    @groups i
            JOIN    tbl_Group g
                ON  g.InternalScopeId = @internalScopeId
                    AND g.DisplayName = i.Name COLLATE ' + @collation + '
            WHERE   g.PartitionId = @partitionId
                    AND g.Active = 1
                    AND g.SpecialType<>(5)
            OPTION (OPTIMIZE FOR (@partitionId UNKNOWN))
        '
        EXEC SP_EXECUTESQL @findDuplicateSQL, @findDuplicateParametersDefinition, @groups = @groups, @internalScopeId = @internalScopeId, @partitionId = @partitionId, @duplicateDisplayNameOUT = @duplicateDisplayName OUTPUT;
        IF (@duplicateDisplayName IS NOT NULL)
        BEGIN
            ROLLBACK TRAN
            SET @tfError = dbo.func_GetMessage(400001); RAISERROR(@tfError, 16, -1, @procedureName, @conflictingScope, @duplicateDisplayName)
            RETURN 400001
        END
    END

    -- Provision a new group sequence ID value
    EXEC @status = prc_iCounterGetNext @partitionId = @partitionId,
                                       @counterName = N'GroupAudit',
                                       @countToReserve = 1,
                                       @firstIdToUse = @sequenceId OUTPUT,
                                       @populateIfMissing = 1

    IF (@status <> 0)
    BEGIN
        ROLLBACK TRAN
        SET @tfError = dbo.func_GetMessage(800001); RAISERROR(@tfError, 16, -1, @procedureName, @status, N'EXEC', N'prc_iCounterGetNext')
        RETURN 800001
    END

    SET XACT_ABORT OFF
    BEGIN TRY
        -- If request is to create a group of type 5 (AzureActiveDirectory groups) and the group already exists in soft deleted state (Active = 0)
        -- then rename Sid of existing deleted group to avoid conflict.

        -- update the audit table with the inactive groups whose sids are being renamed
        MERGE    tbl_Group AS g
        USING    (SELECT @partitionId, Sid, SpecialType FROM @groups) AS src (PartitionId, Sid, SpecialType)
        ON       g.PartitionId = src.PartitionId
                 AND g.Sid = src.Sid
        WHEN MATCHED AND g.SpecialType = src.SpecialType
                     AND g.SpecialType = 5
                     AND g.Active = 0
        THEN UPDATE SET Sid = g.Sid + '-' + CAST(CAST(RAND() * 1000000 AS INT) AS VARCHAR),
                        SequenceId = @sequenceId
        OUTPUT  INSERTED.Sid, INSERTED.Id, INSERTED.DisplayName, INSERTED.SpecialType, INSERTED.ScopeLocal, INSERTED.RestrictedView, INSERTED.Active
        INTO    @updatedGroups (Sid, Id, Name, SpecialType, ScopeLocal, RestrictedView, Active)
        OPTION (OPTIMIZE FOR (@partitionId UNKNOWN));

        INSERT  tbl_Group (PartitionId, InternalScopeId, Sid, Id, SpecialType, DisplayName, Description, VirtualPlugin, RestrictedView, ScopeLocal, Active, SequenceId)
        OUTPUT  INSERTED.Sid, INSERTED.Id, INSERTED.DisplayName, INSERTED.SpecialType, INSERTED.ScopeLocal, INSERTED.RestrictedView, INSERTED.Active
        INTO    @updatedGroups (Sid, Id, Name, SpecialType, ScopeLocal, RestrictedView, Active)
        SELECT  @partitionId,
                @internalScopeId,
                Sid,
                ISNULL(Id, NEWID()),
                SpecialType,
                Name,
                Description,
                VirtualPlugin,
                RestrictedView,
                ScopeLocal,
                Active,
                @sequenceId
        FROM    @groups
        OPTION (OPTIMIZE FOR (@partitionId UNKNOWN))
    END TRY
    BEGIN CATCH
        IF (ERROR_NUMBER()=2601)
        BEGIN
            IF (@errorOnDuplicate = 1)
            BEGIN
                SELECT  TOP 1
                        @conflictingGroup = g.DisplayName
                FROM    @groups i
                JOIN    tbl_Group g
                ON      g.InternalScopeId = @internalScopeId
                        AND g.DisplayName = i.Name
                WHERE   g.PartitionId = @partitionId
                OPTION (OPTIMIZE FOR (@partitionId UNKNOWN))

                ROLLBACK
                SET @tfError = dbo.func_GetMessage(400001); RAISERROR(@tfError, 16, -1, @procedureName, @conflictingScope, @conflictingGroup)
                RETURN 400001
            END
        END
        ELSE
        BEGIN
            ROLLBACK
            SELECT  @errorMessage = dbo.func_FormatErrorMessage(@status, ERROR_MESSAGE(), ERROR_LINE())
            SET @tfError = dbo.func_GetMessage(800200); RAISERROR(@tfError, 16, -1, @errorMessage)
            RETURN 800200
        END
    END CATCH
    SET XACT_ABORT ON

    SELECT  @rootGroupId = Id
    FROM    tbl_Group
    WHERE   PartitionId = @partitionId
            AND InternalScopeId = @internalScopeId
            AND SpecialType = 3
            AND Active = 1
    OPTION (OPTIMIZE FOR (@partitionId UNKNOWN))

    INSERT  tbl_GroupMembership (PartitionId, ContainerId, MemberId, Active, SequenceId)
    OUTPUT  INSERTED.ContainerId, INSERTED.MemberId, INSERTED.Active
    INTO    @updatedGroupMemberships (ContainerId, MemberId, Active)
    SELECT  @partitionId,
            @rootGroupId,
            grp.Id,
            @addActiveScopeMembership,
            @sequenceId
    FROM    @groups groups
    JOIN    tbl_Group grp
    ON      grp.PartitionId = @partitionId
            AND grp.InternalScopeId = @internalScopeId
            AND grp.Sid = groups.Sid
            AND grp.Active = 1
    WHERE   groups.Active = 1
            AND NOT EXISTS (
                SELECT  *
                FROM    tbl_GroupMembership
                WHERE   PartitionId = @partitionId
                        AND ContainerId = @rootGroupId
                        AND MemberId = grp.Id
            )
    OPTION (OPTIMIZE FOR (@partitionId UNKNOWN))

    -- @ids will have all new groups that were created, we need to update records in tbl_GroupScopeVisibility for those groups
    INSERT  @ids (Id)
    SELECT  g.Id
    FROM    @groups g

    EXEC @status = prc_iiUpdateGroupScopeVisibility @partitionId, @ids

    IF (@status <> 0)
    BEGIN
        ROLLBACK TRAN

        DECLARE @id     UNIQUEIDENTIFIER

        SELECT  TOP 1 @id = Id
        FROM    @ids

        DECLARE @msgDataScopeId     NVARCHAR(256) = CONVERT(NVARCHAR(256), @scopeId)
        DECLARE @msgDataUpdateId    NVARCHAR(256) = CONVERT(NVARCHAR(256), @id)

        SET @tfError = dbo.func_GetMessage(400046); RAISERROR(@tfError, 16, -1, @procedureName, @msgDataScopeId, @msgDataUpdateId)
        RETURN 400046
    END

    -- Record changes and issue SQL notifications
    EXEC @status = prc_iiLogGroupChange @partitionId = @partitionId,
                                        @sequenceId = @sequenceId,
                                        @sequenceIdToReturn = @sequenceId OUTPUT,
                                        @eventAuthor = @eventAuthor,
                                        @updatedGroups = @updatedGroups,
                                        @updatedGroupMemberships = @updatedGroupMemberships

    IF (@status <> 0)
    BEGIN
        ROLLBACK TRAN
        SET @tfError = dbo.func_GetMessage(800001); RAISERROR(@tfError, 16, -1, @procedureName, @status, N'EXEC', N'prc_iiLogGroupChange')
        RETURN 800001
    END
    ELSE
    BEGIN
        -- We rollback the transcation in case there were no updates to group audit inorder to restore the counter
        IF (@sequenceId = -1)
        BEGIN
            ROLLBACK TRAN
        END
        ELSE
            BEGIN
            COMMIT TRAN
        END
    END

    -- Return the sequence id
    SELECT  @sequenceId AS SequenceId
END
