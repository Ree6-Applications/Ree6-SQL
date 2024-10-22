CREATE INDEX idx_0b656a7171f415c0665378c5e ON VCLevel (userId, guildId);

CREATE INDEX idx_0df616b5cc4427eb5e3294360 ON Suggestions (channelId, guildId);

CREATE INDEX idx_19996e465c82202aa18a5f903 ON BirthdayWish (userId, guildId);

CREATE INDEX idx_4eabfb36fcb96d367535a664f ON Giveaway (guildId);

CREATE INDEX idx_61180fdd5c7e050bdf1afda91 ON Invites (code, guildId);

CREATE INDEX idx_7a4d8ddfcefc5ec5753a77e0a ON Warning (userId, guildId);

CREATE INDEX idx_7fff13a15f26ab2c95acfc2c4 ON ReactionRole (roleId, guildId);

CREATE INDEX idx_8378fc7abe05674fc6c8ea8f6 ON Opt_out (userId, guildId);

CREATE INDEX idx_8e431ff002adb1348ebe09932 ON Level (userId, guildId);

CREATE INDEX idx_9a30f1a12245ef1f86755cf17 ON Recording (id, guildId);

CREATE INDEX idx_a384068c6a22b1afe3da2a233 ON TemporalVoicechannel (channelId, guildId);

CREATE INDEX idx_b161be6fddaf84985162c1fe3 ON Money_Holder (userId, guildId);

CREATE INDEX idx_de5259bea32e95dcc26373fcd ON Settings (name, guildId);

CREATE INDEX idx_df865b7031c2af9280b22aaea ON Tickets (channelId, guildId);

CREATE INDEX idx_f3d1609a03578e84862863eef ON StreamActions (name, guildId);