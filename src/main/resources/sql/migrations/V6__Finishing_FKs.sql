ALTER TABLE ScheduledMessage
    CHANGE guild guildId BIGINT;

ALTER TABLE Warning
    DROP COLUMN id;

ALTER TABLE ChatProtector
    MODIFY guildId BIGINT NOT NULL;

ALTER TABLE Invites
    MODIFY guildId BIGINT NOT NULL;

ALTER TABLE ReactionRole
    MODIFY guildId BIGINT NOT NULL;

ALTER TABLE StreamActions
    MODIFY guildId BIGINT NOT NULL;

ALTER TABLE ChatProtector
    MODIFY id BIGINT;

ALTER TABLE ChatProtector
    DROP PRIMARY KEY;

ALTER TABLE Invites
    MODIFY id BIGINT;

ALTER TABLE Invites
    DROP PRIMARY KEY;

ALTER TABLE Punishments
    MODIFY id BIGINT;

ALTER TABLE Punishments
    DROP PRIMARY KEY;

ALTER TABLE ReactionRole
    MODIFY id BIGINT;

ALTER TABLE ReactionRole
    DROP PRIMARY KEY;

ALTER TABLE ScheduledMessage
    MODIFY id BIGINT;

ALTER TABLE ScheduledMessage
    DROP PRIMARY KEY;

ALTER TABLE StreamActions
    MODIFY id BIGINT;

ALTER TABLE StreamActions
    DROP PRIMARY KEY;

ALTER TABLE InstagramNotify
    MODIFY webhookId BIGINT NOT NULL;

ALTER TABLE RSSFeed
    MODIFY webhookId BIGINT NOT NULL;

ALTER TABLE RedditNotify
    MODIFY webhookId BIGINT NOT NULL;

ALTER TABLE ScheduledMessageWebhooks
    MODIFY webhookId BIGINT NOT NULL;

ALTER TABLE TikTokNotify
    MODIFY webhookId BIGINT NOT NULL;

ALTER TABLE TwitchNotify
    MODIFY webhookId BIGINT NOT NULL;

ALTER TABLE TwitterNotify
    MODIFY webhookId BIGINT NOT NULL;

ALTER TABLE YouTubeNotify
    MODIFY webhookId BIGINT NOT NULL;

ALTER TABLE ChatProtector
    ADD PRIMARY KEY (id, guildId);

ALTER TABLE ChatProtector
    MODIFY id BIGINT NOT NULL AUTO_INCREMENT;

ALTER TABLE Invites
    ADD PRIMARY KEY (id, guildId);

ALTER TABLE Invites
    MODIFY id BIGINT NOT NULL AUTO_INCREMENT;

ALTER TABLE Punishments
    ADD PRIMARY KEY (id, guildId);

ALTER TABLE Punishments
    MODIFY id BIGINT NOT NULL AUTO_INCREMENT;

ALTER TABLE ReactionRole
    ADD PRIMARY KEY (id, guildId);

ALTER TABLE ReactionRole
    MODIFY id BIGINT NOT NULL AUTO_INCREMENT;

ALTER TABLE ScheduledMessage
    ADD PRIMARY KEY (id, guildId);

ALTER TABLE ScheduledMessage
    MODIFY id BIGINT NOT NULL AUTO_INCREMENT;

ALTER TABLE StreamActions
    ADD PRIMARY KEY (id, guildId);

ALTER TABLE StreamActions
    MODIFY id BIGINT NOT NULL AUTO_INCREMENT;

ALTER TABLE Warning
    ADD PRIMARY KEY (guildId, userId);