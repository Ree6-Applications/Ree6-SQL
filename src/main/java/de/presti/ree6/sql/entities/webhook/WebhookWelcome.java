package de.presti.ree6.sql.entities.webhook;

import de.presti.ree6.sql.entities.webhook.base.Webhook;
import jakarta.persistence.Entity;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

/**
 * SQL Entity for the Welcome-Webhooks.
 */
@Entity
@Table(name = "WelcomeWebhooks", indexes = @Index(columnList = "guildId"))
public class WebhookWelcome extends Webhook {

    /**
     * Constructor.
     */
    public WebhookWelcome() {
    }

    /**
     * @inheritDoc
     */
    public WebhookWelcome(long guildId, long channelId, long webhookId, String token) {
        super(guildId, channelId, webhookId, token);
    }
}
