import {
  Client,
  GatewayIntentBits,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  AuditLogEvent,
  MessageFlags,
} from "discord.js";
import * as dotenv from "dotenv";
import AsyncLock from "async-lock";
import { debounce } from "lodash-es";

import { initializeDb, getIngameName } from "./utils/db.js";
import { Logger } from "./utils/logger.js";

dotenv.config();
const roleUpdateLocks = new AsyncLock();
const ROLE_UPDATE_LOCK_PREFIX = "role_update_";
const pendingUpdates = new Set();
const migratingThreads = new Set();

const requiredEnvVars = ["TOKEN", "DRY_RUN"];
requiredEnvVars.forEach((varName) => {
  if (!process.env[varName]) {
    throw new Error(`Missing required environment variable: ${varName}`);
  }
});

const DRY_RUN = process.env.DRY_RUN === "true";
const MAIN_SERVER_ID = "1309266911703334952";
const OPEN_REVIEW_CHANNEL = "1316198871462051900";
const NOTIFICATIONS_CHANNEL = "1309287447863099486";
const ANNOUNCEMENTS_CHANNEL = "1309287447863099486";
const ADMIN_USER_ID = "107391298171891712";
const FILTER_ROLE_IDS = ["1309271313398894643", "1309284427553312769"]; // Leadership & Officer
const MASTER_LEAD_ROLE_ID = "1330342071126593616"; // Class Lead

const reviewThreads = new Map();

const REVIEW_CHANNELS = {
  tank: {
    channelId: "1316181886308978788",
    leadRoleId: "1311763115733418136", // Tank Lead
    classRoleIds: [
      "1315087293408739401", // SNS / GS
      "1315087506105958420", // SNS / Wand
      "1315087805650571366", // SNS / Dagger
      "1323213957195894805", // SNS / Spear
    ],
    weaponLeadRoleIds: {
      "1315087293408739401": "1323121646336479253", // SNS / GS Lead
      "1315087506105958420": "1323121710861516901", // SNS / Wand Lead
      "1315087805650571366": "1323121684147994756", // SNS / Dagger Lead
      "1323213957195894805": "1324201709886509107", // SNS / Spear Lead
    },
  },
  healer: {
    channelId: "1316182043012632626",
    leadRoleId: "1311763190828371978", // Healer Lead
    classRoleIds: [
      "1315090429233991812", // Wand / Bow
      "1315090436703912058", // Wand / Staff
      "1315090738500993115", // Wand / SNS
      "1315091030248263690", // Wand / Dagger
    ],
    weaponLeadRoleIds: {
      "1315090429233991812": "1323122250995597442", // Wand / Bow Lead
      "1315090436703912058": "1323122341995348078", // Wand / Staff Lead
      "1315090738500993115": "1323122486396715101", // Wand / SNS Lead
      "1315091030248263690": "1323122572174299160", // Wand / Dagger Lead
    },
  },
  ranged: {
    channelId: "1316182011362414693",
    leadRoleId: "1311763275293130783", // Ranged Lead
    classRoleIds: [
      "1315091763370786898", // Staff / Bow
      "1315091966303797248", // Staff / Dagger
      "1315092313755881573", // Bow / Dagger
    ],
    weaponLeadRoleIds: {
      "1315091763370786898": "1323122828802920479", // Staff / Bow Lead
      "1315091966303797248": "1323122917466181672", // Staff / Dagger Lead
      "1315092313755881573": "1323122947040219166", // Bow / Dagger Lead
    },
  },
  melee: {
    channelId: "1316181992177668237",
    leadRoleId: "1315188182404300861", // Melee Lead
    classRoleIds: [
      "1315092445930717194", // GS / Dagger
      "1323213919002689559", // Spear / Dagger
      "1315093022483939338", // Spear / GS
    ],
    weaponLeadRoleIds: {
      "1315092445930717194": "1323123053793640560", // GS / Dagger Lead
      "1323213919002689559": "1323123139500048384", // Spear / Dagger Lead
      "1315093022483939338": "1324201778190880799", // Spear / GS Lead
    },
  },
  bomber: {
    channelId: "1316182023433486427",
    leadRoleId: "1315188211097534495", // Bomber Lead
    classRoleIds: [
      "1315092575509807215", // Dagger / Wand
      "1315092852690128907", // Xbow / Dagger
    ],
    weaponLeadRoleIds: {
      "1315092575509807215": "1323123176405729393", // Dagger / Wand Lead
      "1315092852690128907": "1323123243959451671", // Xbow / Dagger Lead
    },
  },
};

const GUILD_ROLES = {
  GUILD1: { id: "1315072149173698580", name: "Tsunami" },
  GUILD2: { id: "1315071746721976363", name: "Hurricane" },
  GUILD3: { id: "1314816353797935214", name: "Avalanche" },
  GUILD4: { id: "1315072176839327846", name: "Hailstorm" },
};

const WEAPON_TO_CLASS = Object.entries(REVIEW_CHANNELS).reduce(
  (acc, [className, data]) => {
    data.classRoleIds.forEach((roleId) => (acc[roleId] = className));
    return acc;
  },
  {}
);

const getClassForWeapon = (roleId) => WEAPON_TO_CLASS[roleId];

const detectWeaponRoleChanges = (oldMember, newMember) => {
  const oldWeaponRoles = oldMember.roles.cache.filter(
    (role) => WEAPON_TO_CLASS[role.id]
  );
  const newWeaponRoles = newMember.roles.cache.filter(
    (role) => WEAPON_TO_CLASS[role.id]
  );

  return {
    added: newWeaponRoles.filter((r) => !oldWeaponRoles.has(r.id)),
    removed: oldWeaponRoles.filter((r) => !newWeaponRoles.has(r.id)),
  };
};

const detectGuildRoleChanges = (oldMember, newMember) => {
  const oldGuildRoles = oldMember.roles.cache.filter((role) =>
    Object.values(GUILD_ROLES).some((guild) => guild.id === role.id)
  );
  const newGuildRoles = newMember.roles.cache.filter((role) =>
    Object.values(GUILD_ROLES).some((guild) => guild.id === role.id)
  );

  return {
    added: newGuildRoles.filter((r) => !oldGuildRoles.has(r.id)),
    removed: oldGuildRoles.filter((r) => !newGuildRoles.has(r.id)),
  };
};

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildPresences,
    GatewayIntentBits.GuildModeration, // Add this
  ],
});

const createThreadUpdateButton = async (thread, oldClass, newClass, userId) => {
  // Validate classes
  if (!REVIEW_CHANNELS[oldClass] || !REVIEW_CHANNELS[newClass]) {
    Logger.error(
      `Invalid class in thread update: old=${oldClass}, new=${newClass}`
    );
    return;
  }

  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`update_thread_${oldClass}_${newClass}_${userId}`)
      .setLabel(oldClass === newClass ? "Update Thread Name" : "Move Thread")
      .setStyle(ButtonStyle.Primary),
    new ButtonBuilder()
      .setCustomId(`cancel_update_${userId}`)
      .setLabel("Cancel")
      .setStyle(ButtonStyle.Secondary)
  );

  await thread.send({
    content: `Weapon role change detected! Would you like to ${
      oldClass === newClass ? "rename" : "move"
    } this thread to match your new role?\n\nFrom: ${oldClass}\nTo: ${newClass}`,
    components: [row],
  });
};

async function migrateMessages(oldThread, newThread) {
  try {
    Logger.info(
      `Starting migration from thread ${oldThread.id} to ${newThread.id}`
    );

    let allMessages = new Map();
    let lastId = null;

    while (true) {
      const options = { limit: 100 };
      if (lastId) {
        options.before = lastId;
      }

      const messages = await oldThread.messages.fetch(options);
      if (messages.size === 0) break;

      messages.forEach((msg) => allMessages.set(msg.id, msg));
      lastId = messages.last().id;
    }

    let migrated = 0;
    const total = allMessages.size;
    let failedAttachments = 0;

    await newThread.send({
      content: `🔄 Starting migration...`,
      allowedMentions: { parse: [] },
    });

    const messageArray = Array.from(allMessages.values()).reverse();

    for (const message of messageArray) {
      try {
        if (message.system) continue;

        let contentToMigrate, authorToShow;
        if (message.author.id === client.user.id) {
          const migratedMatch = message.content.match(
            /^💬 \*\*(.+?)\*\*: (.+)$/
          );
          if (!migratedMatch) continue;
          [, authorToShow, contentToMigrate] = migratedMatch;
        } else {
          authorToShow = message.author.tag;
          contentToMigrate = message.content;
        }

        // Handle attachments
        if (message.attachments.size > 0) {
          const attachments = Array.from(message.attachments.values());

          // Try to send message with attachments first
          try {
            await newThread.send({
              content: `💬 **${authorToShow}**: ${contentToMigrate}`,
              allowedMentions: { parse: [] },
              files: attachments,
            });

            // Add a small delay after successful attachment send
            await new Promise((resolve) => setTimeout(resolve, 500));
          } catch (attachmentError) {
            Logger.warn(
              `Failed to send attachments for message ${message.id}: ${attachmentError.message}`
            );

            // On failure, send message with URLs
            const attachmentUrls = attachments.map((a) => a.url).join("\n");
            await newThread.send({
              content: `💬 **${authorToShow}**: ${contentToMigrate}\n\n*Attachments:*\n${attachmentUrls}`,
              allowedMentions: { parse: [] },
            });
            failedAttachments += attachments.length;
          }
        } else {
          // No attachments, just send the message
          await newThread.send({
            content: `💬 **${authorToShow}**: ${contentToMigrate}`,
            allowedMentions: { parse: [] },
          });
        }

        migrated++;
      } catch (error) {
        Logger.error(`Failed to migrate message: ${error}`);
        // Continue with next message instead of throwing
      }
    }

    return { migrated, total, failedAttachments };
  } catch (error) {
    Logger.error(`Critical migration error: ${error}`);
    throw error;
  }
}

async function updateThreadState(
  thread,
  userId,
  newChannel,
  className,
  leadRoleId
) {
  try {
    reviewThreads.set(userId, {
      threadId: thread.id,
      channelId: newChannel.id,
      className,
      leadRoleId,
      archived: false,
      locked: false,
    });

    migratingThreads.add(thread.id);
    await thread.delete().catch((error) => {
      Logger.error(`Failed to delete thread: ${error}`);
      throw error;
    });
  } catch (error) {
    // Revert state changes on error
    reviewThreads.delete(userId);
    migratingThreads.delete(thread.id);
    throw error;
  } finally {
    // Ensure cleanup happens
    setTimeout(() => migratingThreads.delete(thread.id), 5000);
  }
}

async function validateThreadAccess(userId, interaction, isAdmin = false) {
  if (!isAdmin && interaction.user.id !== userId) {
    throw new Error("You can only update your own thread.");
  }

  const review = reviewThreads.get(userId);
  if (!review || review.archived || review.locked) {
    throw new Error("No active review thread found.");
  }

  return review;
}

const handleThreadUpdate = async (
  interaction,
  oldClass,
  newClass,
  userId,
  isAdmin = false
) => {
  let createdThread = null;
  const oldThreadId = interaction.channel?.id;

  if (!interaction.deferred) {
    if (interaction.isButton()) {
      await interaction.deferReply({
        flags: MessageFlags.Ephemeral,
      });
    } else {
      await interaction.deferReply();
    }
  }

  try {
    await validateThreadAccess(userId, interaction, isAdmin);

    if (oldClass === newClass) {
      // Handle rename
      const weaponRole = interaction.member.roles.cache.find(
        (r) => WEAPON_TO_CLASS[r.id] === newClass
      );
      if (!weaponRole) {
        throw new Error("Could not find matching weapon role.");
      }

      const wasRenamed = await handleThreadRename(
        interaction.channel,
        userId,
        weaponRole
      );

      await interaction.editReply(
        wasRenamed
          ? "Thread name updated successfully!"
          : "Thread is already up to date!"
      );
      return;
    }

    // Handle migration
    await interaction.editReply("Starting thread migration...");
    const newChannel = interaction.guild.channels.cache.get(
      REVIEW_CHANNELS[newClass].channelId
    );

    const ingameName =
      (await getIngameName(userId)) ||
      (await interaction.guild.members.fetch(userId)).displayName;

    createdThread = await handleReviewMigrationCreation(
      interaction.guild,
      userId,
      [newClass, REVIEW_CHANNELS[newClass]],
      newChannel,
      ingameName
    );

    if (!createdThread) {
      throw new Error("Could not create new thread");
    }

    // First, update the review state to track the new thread
    reviewThreads.set(userId, {
      threadId: createdThread.id,
      channelId: newChannel.id,
      className: newClass,
      leadRoleId: REVIEW_CHANNELS[newClass].leadRoleId,
      archived: false,
      locked: false,
    });

    // Add a delay before migration to ensure thread is ready
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Then migrate messages
    const { migrated, total, failedAttachments } = await migrateMessages(
      interaction.channel,
      createdThread
    ).catch((error) => {
      throw error;
    });

    // After migration is complete, sync thread members
    await syncThreadMembers(createdThread, userId, newClass);

    // Add migration stats to completion message
    let migrationStatus = `✅ Migration complete!\n• ${migrated}/${total} messages transferred`;
    if (failedAttachments > 0) {
      migrationStatus += `\n• ⚠️ ${failedAttachments} attachments could not be transferred (URLs included in messages)`;
    }
    migrationStatus += `\n• Review moved from ${oldClass} to ${newClass}`;

    await createdThread.send({
      content: migrationStatus,
      allowedMentions: { parse: [] },
    });

    // Verify migration was successful
    const oldMessages = await interaction.channel.messages.fetch({
      limit: 100,
    });
    const newMessages = await createdThread.messages.fetch({ limit: 100 });

    // Count attachments in both threads
    const oldAttachmentCount = Array.from(oldMessages.values()).reduce(
      (count, msg) => count + msg.attachments.size,
      0
    );
    const newAttachmentCount = Array.from(newMessages.values()).reduce(
      (count, msg) => count + msg.attachments.size,
      0
    );

    // Add migration stats to completion message
    if (newAttachmentCount < oldAttachmentCount - failedAttachments) {
      await createdThread.send({
        content: `⚠️ Warning: Some attachments may have been missed during migration.\nOriginal thread had ${oldAttachmentCount} attachments, new thread has ${newAttachmentCount}.\nPlease verify all important attachments were transferred.`,
        allowedMentions: { parse: [] },
      });
      Logger.warn(
        `Migration attachment mismatch for ${userId}: ${newAttachmentCount}/${oldAttachmentCount} attachments transferred`
      );
    }

    // Add a delay before deleting the old thread to ensure all messages are processed
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // Mark the thread as being migrated before deleting
    if (oldThreadId) migratingThreads.add(oldThreadId);

    // Delete the old thread last
    await interaction.channel.delete().catch((error) => {
      Logger.error(`Failed to delete old thread: ${error}`);
      // Don't throw here, the migration was successful
    });

    // Update embeds for both old and new channels
    await Promise.all([
      updateClassChannelEmbed(
        interaction.guild,
        oldClass,
        REVIEW_CHANNELS[oldClass]
      ),
      updateClassChannelEmbed(
        interaction.guild,
        newClass,
        REVIEW_CHANNELS[newClass]
      ),
    ]);

    Logger.thread(
      `Migrated review thread for ${userId} from ${oldClass} to ${newClass}`
    );
  } catch (error) {
    pendingUpdates.delete(userId);
    if (oldThreadId) migratingThreads.delete(oldThreadId);
    if (createdThread) {
      await createdThread.delete().catch(() => {}); // Silent cleanup
    }
    if (oldReview && oldReview.className !== oldClass) {
      reviewThreads.set(userId, {
        ...oldReview,
        className: oldClass,
      });
    }
    throw error;
  } finally {
    // Ensure cleanup happens
    if (oldThreadId) {
      setTimeout(() => migratingThreads.delete(oldThreadId), 5000);
    }
  }
};

const validateReviewThreads = async () => {
  try {
    const validationPromises = Array.from(reviewThreads.entries()).map(
      async ([userId, review]) => {
        try {
          const channel = await client.channels
            .fetch(review.channelId)
            .catch(() => null);
          if (!channel) {
            reviewThreads.delete(userId);
            return;
          }

          const thread = await channel.threads
            .fetch(review.threadId)
            .catch(() => null);
          if (!thread) {
            reviewThreads.delete(userId);
            return;
          }

          if (thread.archived || thread.locked) {
            review.archived = thread.archived;
            review.locked = thread.locked;
            review.archivedAt = thread.archived ? Date.now() : null;
            reviewThreads.set(userId, review);
          }
        } catch {
          // Silently continue - errors here are expected and handled
        }
      }
    );

    await Promise.allSettled(validationPromises);
  } catch (error) {
    Logger.error(`Critical error in review validation: ${error}`);
    throw error;
  }
};

async function retryOperation(operation, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await operation();
    } catch (error) {
      if (i === maxRetries - 1) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1000 * (i + 1)));
      Logger.warn(`Retrying operation, attempt ${i + 2}/${maxRetries}`);
    }
  }
}

const isReviewChannel = (channelId) => {
  return Object.values(REVIEW_CHANNELS).some(
    (channel) => channel.channelId === channelId
  );
};

const formatThreadName = (displayName, classRole, userId) => {
  return `${displayName || "Unknown"} - ${classRole} Review [${userId}]`;
};

const checkThreadUpdate = async (userId, guild, existingReview) => {
  const member = await guild.members.fetch(userId);
  const weaponRoles = member.roles.cache.filter(
    (role) => WEAPON_TO_CLASS[role.id]
  );

  // Validate weapon roles
  if (weaponRoles.size === 0) {
    throw new Error("No weapon role found");
  }
  if (weaponRoles.size > 1) {
    throw new Error("Multiple weapon roles found");
  }

  const currentClass = getClassForWeapon(weaponRoles.first().id);
  const oldClass = existingReview.className;

  return {
    currentClass,
    oldClass,
    weaponRole: weaponRoles.first(),
    requiresMigration: currentClass !== oldClass,
  };
};

const logThreadMemberChange = (action, member, thread, reason) => {
  const threadOwner = thread.name.match(/\[(\d+)\]$/)?.[1];
  const threadClass = thread.parent?.name || "unknown";
  Logger.info(
    `[Thread Member ${action}] ${member.user.tag} (${member.id})\n` +
      `Thread: ${thread.name} (${thread.id})\n` +
      `Owner: ${threadOwner}\n` +
      `Class: ${threadClass}\n` +
      `Reason: ${reason}`
  );
};

const updateThreadWeaponLeads = async (
  thread,
  userId,
  weaponRole,
  className
) => {
  try {
    const classData = REVIEW_CHANNELS[className];
    if (!classData?.weaponLeadRoleIds) return;

    // Get the weapon lead role ID for this weapon role
    const weaponLeadRoleId = classData.weaponLeadRoleIds[weaponRole.id];
    if (!weaponLeadRoleId) {
      Logger.warn(`No weapon lead role found for weapon ${weaponRole.name}`);
      return;
    }

    // Get current thread members and guild members in one fetch
    const [threadMembers, guildMembers] = await Promise.all([
      thread.members.fetch(),
      thread.guild.members.fetch(),
    ]);

    // Get all weapon leads first
    const weaponLeadMembers = guildMembers.filter((m) =>
      m.roles.cache.has(weaponLeadRoleId)
    );
    const weaponLeadMemberIds = new Set(weaponLeadMembers.keys());

    // Process each thread member
    for (const [memberId, threadMember] of threadMembers) {
      // Skip the thread owner and members we can't find
      if (memberId === userId) continue;

      const guildMember = guildMembers.get(memberId);
      if (!guildMember) continue;

      try {
        // Only check members who have the class lead role
        if (guildMember.roles.cache.has(classData.leadRoleId)) {
          // If they don't have the specific weapon lead role, remove them
          if (!weaponLeadMemberIds.has(memberId)) {
            await thread.members.remove(memberId);
            logThreadMemberChange(
              "Removed",
              guildMember,
              thread,
              `Class lead without matching weapon lead role (${weaponRole.name})`
            );
          } else {
            logThreadMemberChange(
              "Kept",
              guildMember,
              thread,
              `Class lead with matching weapon lead role (${weaponRole.name})`
            );
          }
        }
      } catch (error) {
        Logger.error(
          `Error processing member ${memberId} in thread ${thread.name}: ${error.message}`
        );
      }
    }

    // Add any missing weapon leads
    const threadMemberIds = new Set(threadMembers.keys());
    await Promise.all(
      Array.from(weaponLeadMembers.values())
        .filter(
          (member) => !threadMemberIds.has(member.id) && member.id !== userId
        )
        .map(async (member) => {
          try {
            await thread.members.add(member.id);
            logThreadMemberChange(
              "Added",
              member,
              thread,
              `Missing weapon lead for ${weaponRole.name}`
            );
          } catch (error) {
            Logger.error(
              `Failed to add weapon lead ${member.user.tag} to thread ${thread.name}: ${error.message}`
            );
          }
        })
    );
  } catch (error) {
    Logger.error(`Error updating thread weapon leads: ${error}`);
  }
};

const handleThreadRename = async (thread, userId, weaponRole) => {
  const ingameName = await getIngameName(userId);
  if (ingameName === undefined) {
    Logger.warn(
      `Failed to fetch ingame name for ${userId} from database, skipping thread rename`
    );
    return false;
  }

  const displayName =
    ingameName === null
      ? (await thread.guild.members.fetch(userId)).displayName
      : ingameName;
  const newName = formatThreadName(displayName, weaponRole.name, userId);
  if (thread.name === newName) return false;

  await thread.setName(newName);

  // Update weapon leads in the thread
  const className = getClassForWeapon(weaponRole.id);
  if (className) {
    await updateThreadWeaponLeads(thread, userId, weaponRole, className);
  }

  return true;
};

const handleReviewCreation = async (
  interaction,
  matchingClass,
  channel,
  ingameName
) => {
  const userId = interaction.user.id;

  if (pendingUpdates.has(userId)) {
    await interaction.editReply({
      content: "A thread operation is already in progress. Please wait.",
    });
    return;
  }

  pendingUpdates.add(userId);
  try {
    const [className, classData] = matchingClass;

    // Create the thread
    const thread = await handleReviewMigrationCreation(
      interaction.guild,
      userId,
      [className, classData],
      channel,
      ingameName
    );

    if (!thread) {
      throw new Error("Failed to create thread");
    }

    // Update review state
    reviewThreads.set(userId, {
      threadId: thread.id,
      channelId: channel.id,
      className,
      leadRoleId: classData.leadRoleId,
      archived: false,
      locked: false,
    });

    // Update the class channel embed
    debouncedUpdateEmbed(interaction.guild, className, classData);

    return thread;
  } catch (error) {
    throw error;
  } finally {
    pendingUpdates.delete(userId);
  }
};

const handleReviewClosure = async (thread, interaction) => {
  // Get thread owner ID from thread name
  const userIdMatch = thread.name.match(/\[(\d+)\]$/);
  if (!userIdMatch) {
    Logger.error(`Could not find user ID in thread name: ${thread.name}`);
    await interaction.editReply({
      content: "Error: Could not find thread owner ID.",
    });
    return;
  }
  const threadOwnerId = userIdMatch[1];

  // Check permissions - allow thread owner, admins, and class leads
  const review = reviewThreads.get(threadOwnerId);
  if (!review) {
    await interaction.editReply({
      content: "Error: Could not find review data.",
    });
    return;
  }

  const isAdmin = interaction.member.permissions.has("Administrator");
  const isClassLead = interaction.member.roles.cache.has(review.leadRoleId);
  const isOwner = interaction.user.id === threadOwnerId;

  if (!isAdmin && !isClassLead && !isOwner) {
    await interaction.editReply({
      content:
        "You don't have permission to close this review thread. Only the thread owner, class leads, or administrators can close threads.",
    });
    return;
  }

  // Get current thread state
  const freshThread = await thread.fetch();
  if (freshThread.archived && freshThread.locked) {
    await interaction.editReply({
      content: "This thread is already closed.",
    });
    return;
  }

  // Close the thread
  await thread.send({
    content: `This thread was closed by <@${interaction.user.id}> (${
      isAdmin ? "Administrator" : isClassLead ? "Class Lead" : "Thread Owner"
    }).`,
    allowedMentions: { users: [] },
  });
  await thread.setLocked(true);
  await thread.setArchived(true);

  // Update the thread status in reviewThreads
  reviewThreads.set(threadOwnerId, {
    ...review,
    archived: true,
    locked: true,
    archivedAt: Date.now(),
  });

  // Update the embed
  debouncedUpdateEmbed(
    interaction.guild,
    review.className,
    REVIEW_CHANNELS[review.className]
  );
  Logger.info(
    `Thread closed by ${interaction.user.tag} (${
      isAdmin ? "Administrator" : isClassLead ? "Class Lead" : "Thread Owner"
    })`
  );

  await interaction.editReply({
    content: "✅ Review thread closed and archived!",
  });
};

client.on("ready", async () => {
  Logger.info(`Bot logged in as ${client.user.tag}`);
  if (DRY_RUN) Logger.info("Running in DRY RUN mode");

  // Add this at the start of ready event
  client.user.setPresence({
    activities: [{ name: "Forty" }],
    status: "online",
  });

  try {
    await initializeDb();
    Logger.info("Database connected");
  } catch (error) {
    Logger.error(`Database connection failed: ${error.message}`);
  }

  try {
    const mainGuild = await client.guilds.fetch(MAIN_SERVER_ID);
    if (!mainGuild) {
      Logger.error("Could not find main guild");
      return;
    }

    // Add this section for initial role sync
    Logger.info("Starting initial class lead role sync...");
    try {
      const allMembers = await mainGuild.members.fetch();
      for (const member of allMembers.values()) {
        await manageClassLeadRole(member);
      }
      Logger.info("Initial class lead role sync completed");
    } catch (error) {
      Logger.error(`Error during initial class lead role sync: ${error}`);
    }

    const reviewChannel = mainGuild.channels.cache.get(OPEN_REVIEW_CHANNEL);
    if (reviewChannel) {
      const messages = await reviewChannel.messages.fetch({ limit: 100 });
      const existingMessage = messages.find(
        (msg) =>
          msg.author.id === client.user.id &&
          msg.content.includes(
            "Click the button below to open a review thread:"
          )
      );

      if (!existingMessage) {
        const row = new ActionRowBuilder().addComponents(
          new ButtonBuilder()
            .setCustomId("open_review")
            .setLabel("Open Review")
            .setStyle(ButtonStyle.Primary)
        );

        await reviewChannel.send({
          content: "Click the button below to open a review thread:",
          components: [row],
        });
      }
    }

    // Populate review threads from existing threads
    for (const [className, channelData] of Object.entries(REVIEW_CHANNELS)) {
      const channel = mainGuild.channels.cache.get(channelData.channelId);
      if (!channel) continue;

      const [activeThreads, archivedThreads] = await Promise.all([
        channel.threads.fetchActive(),
        channel.threads.fetchArchived({
          fetchAll: true,
          type: "private",
        }),
      ]);

      const allThreads = [
        ...activeThreads.threads.values(),
        ...archivedThreads.threads.values(),
      ];

      const userThreads = new Map();

      allThreads.forEach(async (thread) => {
        const userIdMatch = thread.name.match(/\[(\d+)\]$/);
        if (!userIdMatch) return;

        const userId = userIdMatch[1];
        const existingThread = userThreads.get(userId);

        if (existingThread) {
          try {
            const member = await mainGuild.members.fetch(userId);
            const displayName = member.nickname || member.user.username;
            const notificationChannel = mainGuild.channels.cache.get(
              NOTIFICATIONS_CHANNEL
            );

            Logger.warn(
              `User ${userId} (${displayName}) has multiple threads:\nExisting: <#${existingThread.threadId}>\nNew: <#${thread.id}>`
            );

            if (notificationChannel) {
              await notificationChannel.send({
                content: `⚠️ Multiple threads detected for ${displayName} (${userId}):\nExisting: <#${existingThread.threadId}>\nNew: <#${thread.id}>`,
              });
            }
          } catch (error) {
            Logger.warn(
              `User ${userId} (user left server) has multiple threads:\nExisting: <#${existingThread.threadId}>\nNew: <#${thread.id}>`
            );
          }
          if (!existingThread.archived && thread.archived) {
            return;
          }
        }

        userThreads.set(userId, {
          threadId: thread.id,
          channelId: channel.id,
          className: className,
          leadRoleId: channelData.leadRoleId,
          archived: thread.archived,
        });
      });

      userThreads.forEach((value, key) => {
        reviewThreads.set(key, value);
      });
    }

    Logger.info("All reviews populated from existing threads");

    // Sync weapon leads on startup - ONE TIME UNLESS NEEDED
    //try {
    //  Logger.info("Starting initial weapon lead sync...");
    //  await syncLeadRoleAccess(mainGuild);
    //} catch (error) {
    //  Logger.error(`Error during initial weapon lead sync: ${error}`);
    //}

    // Add cleanup call here, before the regular validation starts
    // await cleanupDuplicateMigrationMessages(mainGuild); // ONE TIME USE, NOT NEEDED CURRENTLY

    try {
      await client.application.commands.create({
        name: "checkthread",
        description: "Check if current thread needs class update",
        type: 1,
        defaultMemberPermissions: ["Administrator"],
      });
      Logger.info("Registered /checkthread command");

      await client.application.commands.create({
        name: "cleanthreads",
        description: "Close all review threads for users no longer in a guild",
        type: 1,
        defaultMemberPermissions: ["Administrator"],
      });
      Logger.info("Registered /cleanthreads command");
    } catch (error) {
      Logger.error(`Error registering commands: ${error}`);
    }

    // Add command handler for /checkthread
    client.on("interactionCreate", async (interaction) => {
      if (!interaction.isCommand()) return;

      switch (interaction.commandName) {
        case "checkthread":
          await interaction.deferReply();

          try {
            // Check administrator permission
            if (!interaction.member.permissions.has("Administrator")) {
              await interaction.editReply(
                "This command can only be used by administrators."
              );
              return;
            }

            // Check if we're in a thread
            if (!interaction.channel.isThread()) {
              await interaction.editReply(
                "This command can only be used in a thread."
              );
              return;
            }

            // Get thread owner ID from thread name
            const userIdMatch = interaction.channel.name.match(/\[(\d+)\]$/);
            if (!userIdMatch) {
              await interaction.editReply(
                "Could not find thread owner ID in thread name."
              );
              return;
            }

            const userId = userIdMatch[1];
            const review = reviewThreads.get(userId);

            if (!review) {
              await interaction.editReply(
                "This thread is not being tracked as a review thread."
              );
              return;
            }

            // Get member's current weapon roles
            const member = await interaction.guild.members.fetch(userId);
            const weaponRoles = member.roles.cache.filter(
              (role) => WEAPON_TO_CLASS[role.id]
            );

            if (weaponRoles.size !== 1) {
              await interaction.editReply(
                "User must have exactly one weapon role to migrate thread."
              );
              return;
            }

            const currentClass = getClassForWeapon(weaponRoles.first().id);
            if (currentClass === review.className) {
              // Thread is in correct class, check if members need resyncing
              const thread = interaction.channel;
              const threadMembers = await thread.members.fetch();
              const guild = thread.guild;

              // Get the thread owner
              const threadOwner = await guild.members.fetch(userId);

              // Get the weapon leads that should be in the thread
              const weaponRole = threadOwner.roles.cache.find((role) =>
                REVIEW_CHANNELS[currentClass].classRoleIds.includes(role.id)
              );

              const weaponLeadRoleId = weaponRole
                ? REVIEW_CHANNELS[currentClass].weaponLeadRoleIds?.[
                    weaponRole.id
                  ]
                : null;

              const expectedWeaponLeads = weaponLeadRoleId
                ? (await guild.members.fetch()).filter(
                    (m) =>
                      m.roles.cache.has(weaponLeadRoleId) && m.id !== userId
                  )
                : new Map();

              // Check if current members match expected members
              const currentMemberIds = new Set(threadMembers.keys());
              const expectedMemberIds = new Set([
                userId,
                ...expectedWeaponLeads.keys(),
              ]);

              // Check if ANY member is unexpected or missing
              const hasUnexpectedMembers = [...currentMemberIds].some(
                (id) => !expectedMemberIds.has(id)
              );
              const hasMissingMembers = [...expectedMemberIds].some(
                (id) => !currentMemberIds.has(id)
              );

              if (hasUnexpectedMembers || hasMissingMembers) {
                await syncThreadMembers(thread, userId, currentClass);
                await interaction.editReply(
                  "Thread members have been resynced to match current roles."
                );
              } else {
                await interaction.editReply(
                  "Thread members are already correctly synced!"
                );
              }
              return;
            }

            // Force thread migration
            await handleThreadUpdate(
              interaction,
              review.className,
              currentClass,
              userId,
              true
            );
          } catch (error) {
            Logger.error(`Error in checkthread command: ${error}`);
            await interaction.editReply(
              "An error occurred while processing the thread migration."
            );
          }
          break;

        case "cleanthreads":
          // Check administrator permission
          if (!interaction.member.permissions.has("Administrator")) {
            await interaction.reply({
              content: "This command can only be used by administrators.",
              ephemeral: true,
            });
            return;
          }

          await interaction.deferReply();

          try {
            const guild = interaction.guild;
            const guildRoleIds = Object.values(GUILD_ROLES).map(
              (role) => role.id
            );
            let closedCount = 0;
            let skippedCount = 0;
            let errorCount = 0;

            await interaction.editReply(
              "Starting to close threads for users no longer in guild..."
            );

            // Process all review threads
            for (const [userId, review] of reviewThreads.entries()) {
              // Skip already archived/locked threads
              if (review.archived || review.locked) {
                skippedCount++;
                continue;
              }

              try {
                // Get member and check guild roles
                const member = await guild.members
                  .fetch(userId)
                  .catch(() => null);
                const hasGuildRole =
                  member &&
                  guildRoleIds.some((roleId) => member.roles.cache.has(roleId));
                const hasLeadershipRole =
                  member &&
                  FILTER_ROLE_IDS.some((roleId) =>
                    member.roles.cache.has(roleId)
                  );

                // Skip if they have leadership role or guild role
                if (hasLeadershipRole || hasGuildRole) {
                  skippedCount++;
                  continue;
                }

                // Get thread
                const channel = await guild.channels.fetch(review.channelId);
                const thread = await channel?.threads
                  .fetch(review.threadId)
                  .catch(() => null);

                if (!thread) {
                  errorCount++;
                  continue;
                }

                // Close thread using existing system
                await thread.send({
                  content: `Thread closed automatically - User no longer has a guild role.\nClosed by: ${interaction.user.tag} (Administrator)`,
                  allowedMentions: { parse: [] },
                });

                await thread.setLocked(true);
                await thread.setArchived(true);

                // Update review state
                reviewThreads.set(userId, {
                  ...review,
                  archived: true,
                  locked: true,
                  archivedAt: Date.now(),
                });

                // Update the embed
                debouncedUpdateEmbed(
                  guild,
                  review.className,
                  REVIEW_CHANNELS[review.className]
                );

                closedCount++;
                Logger.info(`Auto-closed thread for ${userId} (no guild role)`);

                // Add a small delay to avoid rate limits
                await new Promise((resolve) => setTimeout(resolve, 1000));

                // Update progress every 5 threads
                if (closedCount % 5 === 0) {
                  await interaction.editReply(
                    `Progress update:\n` +
                      `• Closed: ${closedCount} threads\n` +
                      `• Skipped: ${skippedCount} threads\n` +
                      `• Errors: ${errorCount} threads\n` +
                      `Still processing...`
                  );
                }
              } catch (error) {
                Logger.error(`Error processing thread for ${userId}: ${error}`);
                errorCount++;
              }
            }

            // Send final summary
            await interaction.editReply(
              `Operation complete!\n` +
                `• Closed: ${closedCount} threads\n` +
                `• Skipped: ${skippedCount} threads (already closed or has valid roles)\n` +
                `• Errors: ${errorCount} threads`
            );
          } catch (error) {
            Logger.error(`Error in cleanthreads command: ${error}`);
            await interaction.editReply(
              "An error occurred while processing the command."
            );
          }
          break;
      }
    });

    // Validate weapon roles and update embeds in one pass
    Logger.info("Starting weapon role validation and embed updates...");
    try {
      await validateReviewWeaponRoles(mainGuild);
    } catch (error) {
      Logger.error(`Error during weapon role validation: ${error}`);
    }

    let isValidating = false;
    setInterval(async () => {
      if (isValidating) {
        Logger.info(
          "Skipping review validation - previous validation still running"
        );
        return;
      }
      isValidating = true;
      try {
        const mainGuild = await client.guilds.fetch(MAIN_SERVER_ID);
        await validateReviewThreads();
        await validateThreadNames(mainGuild);
      } catch (error) {
        Logger.error(`Error in periodic validation: ${error}`);
      } finally {
        isValidating = false;
        // Force reset isValidating after 10 minutes
        setTimeout(() => {
          isValidating = false;
        }, 10 * 60 * 1000);
      }
    }, 6 * 60 * 60 * 1000); // Run every 6 hours

    // Single cleanup interval for all temporary state
    setInterval(() => {
      // Clean up pending updates
      pendingUpdates.forEach((userId) => {
        const review = reviewThreads.get(userId);
        if (!review || review.archived || review.locked) {
          pendingUpdates.delete(userId);
        }
      });

      // Clean up migrating threads
      migratingThreads.clear();
    }, 24 * 60 * 60 * 1000); // Run once per day

    // Clear any existing state on startup
    pendingUpdates.clear();
    migratingThreads.clear();

    // Cancel any pending debounced operations
    debouncedUpdateEmbed.cancel();
    debouncedHandleRoleUpdate.cancel();
  } catch (error) {
    Logger.error(`Error during ready event: ${error}`);
  }
});

// Handle members leaving the server (leaving, kicked, or banned)
client.on("guildMemberRemove", async (member) => {
  try {
    const review = reviewThreads.get(member.id);
    if (!review || review.archived || review.locked) return;

    const channel = await member.guild.channels.fetch(review.channelId);
    if (!channel) return;

    const thread = await channel.threads.fetch(review.threadId);
    if (!thread) return;

    // Send notification and close thread
    await thread.send({
      content: `Thread closed automatically - ${member.user.tag} (${member.id}) has left the server.`,
      allowedMentions: { parse: [] },
    });

    await thread.setLocked(true);
    await thread.setArchived(true);

    // Update review state
    reviewThreads.set(member.id, {
      ...review,
      archived: true,
      locked: true,
      archivedAt: Date.now(),
    });

    // Update the embed
    await updateClassChannelEmbed(
      member.guild,
      review.className,
      REVIEW_CHANNELS[review.className]
    );

    Logger.info(`Closed thread for ${member.user.tag} (left server)`);
  } catch (error) {
    Logger.error(
      `Error handling member leave for ${member.user.tag}: ${error}`
    );
  }
});

// Helper functions for button handlers
const handleOpenReviewButton = async (interaction) => {
  await interaction.deferReply({ flags: MessageFlags.Ephemeral });
  const userId = interaction.user.id;
  const existingReview = reviewThreads.get(userId);

  if (existingReview) {
    const channel = interaction.guild.channels.cache.get(
      existingReview.channelId
    );
    const thread = await channel?.threads
      .fetch(existingReview.threadId)
      .catch(() => null);

    if (thread) {
      // Handle existing thread
      if (thread.archived || thread.locked) {
        try {
          if (thread.archived) await thread.setArchived(false);
          if (thread.locked) await thread.setLocked(false);

          // Sync thread members after reopening
          await syncThreadMembers(thread, userId, existingReview.className);

          reviewThreads.set(userId, {
            ...existingReview,
            archived: false,
            locked: false,
          });

          await thread.send({
            content: "Thread reopened.",
            allowedMentions: { parse: [] },
          });

          debouncedUpdateEmbed(
            interaction.guild,
            existingReview.className,
            REVIEW_CHANNELS[existingReview.className]
          );

          await interaction.editReply({
            content: `Your review thread has been reopened: <#${thread.id}>`,
          });
          return;
        } catch (error) {
          Logger.error(`Failed to reopen thread: ${error}`);
          reviewThreads.delete(userId);
        }
      } else {
        // Thread is open, check if user needs to be re-added
        const threadMember = await thread.members
          .fetch(userId)
          .catch(() => null);
        if (threadMember) {
          await interaction.editReply({
            content: `You already have an active review thread: <#${thread.id}>`,
          });
          return;
        }

        await thread.members.add(userId);
        await interaction.editReply({
          content: `You were re-added to your existing review thread: <#${thread.id}>`,
        });
        return;
      }
    } else {
      reviewThreads.delete(userId);
    }
  }

  // Create new thread
  const userRoles = interaction.member.roles.cache;
  const matchingClass = Object.entries(REVIEW_CHANNELS).find(
    ([, channelData]) =>
      channelData.classRoleIds.some((roleId) => userRoles.has(roleId))
  );

  if (!matchingClass) {
    await interaction.editReply({
      content: "You must have a valid class role to open a review thread.",
    });
    return;
  }

  const ingameName = await getIngameName(userId);
  if (ingameName === undefined) {
    await interaction.editReply({
      content:
        "Failed to fetch your in-game name from the database. Please try again later.",
    });
    return;
  }

  if (ingameName === null) {
    await interaction.editReply({
      content:
        "You need to set your in-game name first: https://discord.com/channels/1309266911703334952/1309279173566664714/1319551325452898386",
    });
    return;
  }

  const [, channelData] = matchingClass;
  const channel = interaction.guild.channels.cache.get(channelData.channelId);

  try {
    Logger.info(`Creating new review thread for ${interaction.user.tag}`);
    const thread = await handleReviewCreation(
      interaction,
      matchingClass,
      channel,
      ingameName
    );

    // Add this line to sync thread members after creation
    await syncThreadMembers(thread, userId, matchingClass[0]);

    await interaction.editReply({
      content: `New review thread created: <#${thread.id}>`,
    });
  } catch (error) {
    Logger.error(`Failed to create review thread: ${error}`);
    await interaction.editReply({
      content: "There was an error creating the review thread.",
    });
  }
};

const handleCloseReviewButton = async (interaction, userId) => {
  await interaction.deferReply({ flags: MessageFlags.Ephemeral });
  const reviewData = reviewThreads.get(userId);

  if (!reviewData || reviewData.archived || reviewData.locked) {
    await interaction.editReply({ content: "No active review found." });
    return;
  }

  const channel = interaction.guild.channels.cache.get(reviewData.channelId);
  if (!channel) {
    await interaction.editReply({ content: "Review channel not found." });
    return;
  }

  try {
    const thread = await channel.threads.fetch(reviewData.threadId);
    if (!thread) {
      await interaction.editReply({ content: "Review thread not found." });
      reviewThreads.delete(userId);
      return;
    }

    await handleReviewClosure(thread, interaction);
  } catch (error) {
    Logger.error(`Error closing review thread: ${error}`);
    await interaction.editReply({
      content: "There was an error closing the review thread.",
    });
  }
};

const handleUpdateThreadButton = async (
  interaction,
  oldClass,
  newClass,
  userId
) => {
  // Remove the deferReply here since it's handled in handleThreadUpdate
  if (interaction.user.id !== userId) {
    await interaction.reply({
      content: "Only the thread owner can use this button.",
      flags: MessageFlags.Ephemeral,
    });
    return;
  }

  await handleThreadUpdate(interaction, oldClass, newClass, userId);
};

const handleCancelUpdateButton = async (interaction, userId) => {
  await interaction.deferReply({ flags: MessageFlags.Ephemeral });
  if (interaction.user.id !== userId) {
    await interaction.reply({
      content: "Only the thread owner can use this button.",
      flags: MessageFlags.Ephemeral,
    });
    return;
  }

  await interaction.message.delete();
  pendingUpdates.delete(userId);
};

// Main button interaction handler
client.on("interactionCreate", async (interaction) => {
  if (!interaction.isButton()) return;

  const [action, review, ...params] = interaction.customId.split("_");

  try {
    switch (interaction.customId) {
      case "open_review":
        await handleOpenReviewButton(interaction);
        break;
      case `close_review_${params[0]}`:
        await handleCloseReviewButton(interaction, params[0]);
        break;
      case `update_thread_${params.join("_")}`:
        await handleUpdateThreadButton(
          interaction,
          params[0],
          params[1],
          params[2]
        );
        break;
      case `cancel_update_${params[0]}`:
        await handleCancelUpdateButton(interaction, params[0]);
        break;
    }
  } catch (error) {
    Logger.error(`Error handling button interaction: ${error}`);
    try {
      const reply = interaction.deferred
        ? interaction.editReply
        : interaction.reply;
      await reply.call(interaction, {
        content: "There was an error processing your request.",
        flags: MessageFlags.Ephemeral,
      });
    } catch (e) {
      Logger.error(`Failed to send error message: ${e}`);
    }
  }
});

const getReviewThreadsByLeadRole = (leadRoleId) =>
  Array.from(reviewThreads.values()).filter((review) => {
    // Check weapon-specific lead roles
    const channelData = REVIEW_CHANNELS[review.className];
    if (channelData?.weaponLeadRoleIds) {
      // Find the weapon role that matches this review
      const weaponRoleId = Object.entries(channelData.weaponLeadRoleIds).find(
        ([classRoleId, weaponLeadRoleId]) =>
          weaponLeadRoleId === leadRoleId &&
          channelData.classRoleIds.includes(classRoleId)
      )?.[0];

      if (weaponRoleId) return true;
    }

    return false;
  });

const updateThreadAccess = async (member, leadRoleId, shouldAdd) => {
  const lockKey = `${ROLE_UPDATE_LOCK_PREFIX}${member.id}`;

  return await roleUpdateLocks.acquire(lockKey, async () => {
    const relevantThreads = getReviewThreadsByLeadRole(leadRoleId);
    const startTime = Date.now();

    Logger.info(
      `Starting thread access updates for ${member.user.tag} (${
        shouldAdd ? "adding to" : "removing from"
      } threads)`
    );

    let completed = [];

    for (const review of relevantThreads) {
      try {
        // Check if operation was superseded
        const currentMember = await member.guild.members.fetch(member.id);
        const hasRoleNow = currentMember.roles.cache.has(leadRoleId);
        if (hasRoleNow !== shouldAdd) {
          Logger.info(
            `Role state changed mid-operation for ${member.user.tag}, reversing completed changes`
          );

          // Reverse completed operations
          for (const { thread, wasAdded } of completed) {
            if (wasAdded) {
              await retryOperation(() => thread.members.remove(member.id));
              logThreadMemberChange(
                "Removed",
                member,
                thread,
                "Reverting due to role state change"
              );
            } else {
              await retryOperation(() => thread.members.add(member.id));
              logThreadMemberChange(
                "Added",
                member,
                thread,
                "Reverting due to role state change"
              );
            }
          }
          return;
        }

        const channel = await client.channels.fetch(review.channelId);
        const thread = await channel.threads.fetch(review.threadId);
        if (!thread) continue;

        const threadMember = await thread.members
          .fetch(member.id)
          .catch(() => null);

        if (shouldAdd && !threadMember) {
          await retryOperation(() => thread.members.add(member.id));
          completed.push({ thread, wasAdded: true });
          logThreadMemberChange(
            "Added",
            member,
            thread,
            `Weapon lead role ${leadRoleId} added`
          );
        } else if (!shouldAdd && threadMember) {
          await retryOperation(() => thread.members.remove(member.id));
          completed.push({ thread, wasAdded: false });
          logThreadMemberChange(
            "Removed",
            member,
            thread,
            `Weapon lead role ${leadRoleId} removed`
          );
        }
      } catch (error) {
        Logger.error(
          `Error updating thread access for ${member.user.tag}: ${error}`
        );
      }
    }

    Logger.info(
      `Completed thread access updates for ${member.user.tag} in ${
        Date.now() - startTime
      }ms`
    );
  });
};

async function checkGuildMembersWithoutReviews(guild) {
  const membersWithGuildRoles = new Set();

  // Get all members with any guild role
  const guildRoleIds = Object.values(GUILD_ROLES).map((role) => role.id);
  const allMembers = await guild.members.fetch();

  allMembers.forEach((member) => {
    if (member.user.bot) return;
    if (FILTER_ROLE_IDS.some((roleId) => member.roles.cache.has(roleId)))
      return; // Skip filtered roles
    if (guildRoleIds.some((roleId) => member.roles.cache.has(roleId))) {
      membersWithGuildRoles.add(member.id);
    }
  });

  // Filter out members who already have review threads
  const membersWithoutReviews = [];
  for (const memberId of membersWithGuildRoles) {
    const review = reviewThreads.get(memberId);
    if (!review || review.archived) {
      try {
        const member = await guild.members.fetch(memberId);
        membersWithoutReviews.push({
          id: memberId,
          tag: member.user.tag,
          displayName: member.displayName,
        });
      } catch (error) {
        Logger.error(`Error fetching member ${memberId}: ${error}`);
      }
    }
  }

  return membersWithoutReviews;
}

const sendMembersWithoutReviews = async (
  guild,
  channel,
  message,
  shouldMention = false
) => {
  await message.reply("Checking for members without review threads...");
  const membersWithoutReviews = await checkGuildMembersWithoutReviews(guild);

  if (membersWithoutReviews.length === 0) {
    await channel.send({
      content: "All guild members have active review threads! 🎉",
    });
    return;
  }

  const chunks = membersWithoutReviews
    .map((m) => `<@${m.id}>`)
    .reduce((acc, mention) => {
      if (!acc.length || (acc[acc.length - 1] + mention).length > 1900) {
        acc.push(mention);
      } else {
        acc[acc.length - 1] += `, ${mention}`;
      }
      return acc;
    }, []);

  await channel.send({
    content: "**Guild members without review threads:**",
  });

  for (const chunk of chunks) {
    await channel.send({
      content: chunk,
      allowedMentions: shouldMention ? { parse: ["users"] } : { users: [] },
    });
  }

  Logger.info(
    `Notified about ${membersWithoutReviews.length} members without reviews`
  );
};

client.on("messageCreate", async (message) => {
  const command = message.content.toLowerCase();
  if (!command.startsWith("!notify")) return;

  if (!message.member.permissions.has("Administrator")) {
    await message.reply({
      content: "You need Administrator permission to use this command.",
      flags: MessageFlags.Ephemeral,
    });
    return;
  }

  try {
    const guild = message.guild;
    let targetChannel;
    let shouldMention = false;

    if (command === "!notifythreads") {
      targetChannel = guild.channels.cache.get(ANNOUNCEMENTS_CHANNEL);
      shouldMention = true;
    } else if (command === "!notifytest") {
      targetChannel = guild.channels.cache.get(NOTIFICATIONS_CHANNEL);
    } else {
      return;
    }

    if (!targetChannel) {
      await message.reply("Target channel not found!");
      return;
    }

    await sendMembersWithoutReviews(
      guild,
      targetChannel,
      message,
      shouldMention
    );
  } catch (error) {
    Logger.error(`Error in notify command: ${error}`);
    await message.reply(
      "There was an error checking for members without reviews."
    );
  }
});

const handleThreadStateChange = async (thread, oldThread, executor, review) => {
  Logger.info(
    `Thread state change by ${executor.tag} - Archive: ${thread.archived}, Lock: ${thread.locked}`
  );

  // If thread is being unarchived or unlocked
  if (
    (!thread.archived && oldThread.archived) ||
    (!thread.locked && oldThread.locked)
  ) {
    await syncThreadMembers(thread, review.userId, review.className);
    await thread.send({
      content: `Thread reopened by ${executor.tag} - Members list has been synced.`,
      allowedMentions: { parse: [] },
    });
  }

  // Update review state
  const updatedReview = {
    ...review,
    archived: thread.archived,
    locked: thread.locked,
    archivedAt: thread.archived ? Date.now() : null,
  };
  reviewThreads.set(review.userId, updatedReview);
  Logger.info(`Updated review state for ${review.userId}`);

  // Update embed
  debouncedUpdateEmbed(
    thread.guild,
    review.className,
    REVIEW_CHANNELS[review.className]
  );
  Logger.info(`Updated ${review.className} embed after state change`);
};

const handleUnauthorizedRename = async (oldThread, newThread, executor) => {
  Logger.warn(
    `Unauthorized rename attempt by ${executor.tag} (${executor.id}): ${oldThread.name} -> ${newThread.name}`
  );

  try {
    await newThread.send({
      content: `Thread name changes are restricted to bot and administrators only.\nAttempted by: <@${executor.id}> (${executor.tag})\n<@${ADMIN_USER_ID}> please revert this change.`,
      allowedMentions: { users: [ADMIN_USER_ID] },
    });
    await newThread.setName(oldThread.name);
    Logger.info(`Reverted unauthorized name change for ${oldThread.name}`);
  } catch (error) {
    Logger.error(`Failed to handle unauthorized rename: ${error}`);
  }
};

client.on("threadUpdate", async (oldThread, newThread) => {
  // Skip if not a review thread
  if (!isReviewChannel(newThread.parentId)) return;

  // Get thread owner ID and review
  const userId = newThread.name.match(/\[(\d+)\]$/)?.[1];
  if (!userId) return;

  const review = reviewThreads.get(userId);
  if (!review) return;

  Logger.info(`Thread update detected for ${oldThread.name} (${userId})`);

  // Get executor of the change
  const updateLog = (
    await newThread.guild.fetchAuditLogs({
      type: AuditLogEvent.ThreadUpdate,
      limit: 1,
    })
  ).entries.first();

  const executor = updateLog?.executor;
  if (!executor) {
    Logger.warn(`No executor found for thread update on ${oldThread.name}`);
    return;
  }

  // Handle state changes (archive/lock)
  const stateChanged =
    oldThread.archived !== newThread.archived ||
    oldThread.locked !== newThread.locked;
  if (stateChanged) {
    await handleThreadStateChange(newThread, oldThread, executor, {
      ...review,
      userId,
    });
    return;
  }

  // Handle name changes
  const nameChanged = oldThread.name !== newThread.name;
  if (
    nameChanged &&
    executor.id !== client.user.id &&
    executor.id !== ADMIN_USER_ID
  ) {
    await handleUnauthorizedRename(oldThread, newThread, executor);
  }
});

client.on("threadDelete", async (thread) => {
  // Skip if this is a planned migration
  if (migratingThreads.has(thread.id)) return;

  // Check if this is a review thread and remove it from tracking
  const userId = thread.name.match(/\[(\d+)\]$/)?.[1];
  if (userId && reviewThreads.has(userId)) {
    reviewThreads.delete(userId);
    Logger.info(
      `Removed review thread for ${userId} from tracking (thread deleted)`
    );
  }
});

// Helper function to get weapon leads for a role
const getWeaponLeadsForRole = async (guild, weaponLeadRoleId) => {
  return (await guild.members.fetch()).filter((m) =>
    m.roles.cache.has(weaponLeadRoleId)
  );
};

// Helper function to sync thread weapon leads
const syncThreadWeaponLeads = async (
  thread,
  threadOwner,
  weaponLeadRoleId,
  className
) => {
  try {
    const weaponLeadMembers = await getWeaponLeadsForRole(
      thread.guild,
      weaponLeadRoleId
    );
    const threadMembers = await thread.members.fetch();

    // Add any missing weapon leads
    await Promise.allSettled(
      Array.from(weaponLeadMembers.values())
        .filter(
          (member) =>
            !threadMembers.has(member.id) && member.id !== threadOwner.id
        )
        .map(async (member) => {
          await safeThreadMemberUpdate(
            thread,
            member.id,
            "add",
            member,
            `Initial weapon lead access for ${className}`
          );
        })
    );

    Logger.info(
      `Synced ${weaponLeadMembers.size} weapon leads for thread ${thread.name}`
    );
  } catch (error) {
    Logger.error(
      `Error syncing weapon leads for thread ${thread.name}: ${error.message}`
    );
  }
};

// Update syncLeadRoleAccess to skip archived/locked threads
const syncLeadRoleAccess = async (guild) => {
  Logger.info("Starting lead role access sync...");

  try {
    for (const [className, channelData] of Object.entries(REVIEW_CHANNELS)) {
      if (!channelData.weaponLeadRoleIds) continue;

      // Get all ACTIVE threads for this class
      const relevantThreads = Array.from(reviewThreads.entries())
        .filter(
          ([, review]) =>
            review.className === className && !review.archived && !review.locked // Add these filters
        )
        .map(async ([userId, review]) => {
          try {
            const channel = await guild.channels.fetch(review.channelId);
            const thread = await channel.threads.fetch(review.threadId);
            if (!thread) return null;

            const threadOwner = await guild.members
              .fetch(userId)
              .catch(() => null);
            if (!threadOwner) return null;

            // Find the weapon role of the thread owner
            const weaponRole = threadOwner.roles.cache.find((role) =>
              channelData.classRoleIds.includes(role.id)
            );
            if (!weaponRole) return null;

            // Get the corresponding weapon lead role
            const weaponLeadRoleId =
              channelData.weaponLeadRoleIds[weaponRole.id];
            if (!weaponLeadRoleId) return null;

            return {
              thread,
              threadOwner,
              weaponLeadRoleId,
            };
          } catch (error) {
            Logger.error(`Error processing thread in ${className}: ${error}`);
            return null;
          }
        });

      const validThreads = (await Promise.all(relevantThreads)).filter(Boolean);

      // Process each thread
      await Promise.allSettled(
        validThreads.map(({ thread, threadOwner, weaponLeadRoleId }) =>
          syncThreadWeaponLeads(
            thread,
            threadOwner,
            weaponLeadRoleId,
            className
          )
        )
      );
    }

    Logger.info("Lead role access sync completed");
  } catch (error) {
    Logger.error(`Error during lead role sync: ${error}`);
  }
};

const detectRoleChanges = (oldMember, newMember) => {
  const allWeaponLeadRoleIds = Object.values(REVIEW_CHANNELS).flatMap(
    (channel) =>
      channel.weaponLeadRoleIds ? Object.values(channel.weaponLeadRoleIds) : []
  );

  // Get weapon and guild role changes in one pass
  const changes = {
    weapon: { added: new Set(), removed: new Set() },
    guild: { added: new Set(), removed: new Set() },
    weaponLead: { added: new Set(), removed: new Set() },
  };

  // Check removed roles
  oldMember.roles.cache.forEach((role) => {
    if (WEAPON_TO_CLASS[role.id]) {
      if (!newMember.roles.cache.has(role.id)) {
        changes.weapon.removed.add(role);
      }
    } else if (allWeaponLeadRoleIds.includes(role.id)) {
      // Add this check
      if (!newMember.roles.cache.has(role.id)) {
        changes.weaponLead.removed.add(role);
      }
    } else if (
      Object.values(GUILD_ROLES).some((guild) => guild.id === role.id)
    ) {
      if (!newMember.roles.cache.has(role.id)) {
        changes.guild.removed.add(role);
      }
    }
  });

  // Check added roles
  newMember.roles.cache.forEach((role) => {
    if (WEAPON_TO_CLASS[role.id]) {
      if (!oldMember.roles.cache.has(role.id)) {
        changes.weapon.added.add(role);
      }
    } else if (allWeaponLeadRoleIds.includes(role.id)) {
      // Add this check
      if (!oldMember.roles.cache.has(role.id)) {
        changes.weaponLead.added.add(role);
      }
    } else if (
      Object.values(GUILD_ROLES).some((guild) => guild.id === role.id)
    ) {
      if (!oldMember.roles.cache.has(role.id)) {
        changes.guild.added.add(role);
      }
    }
  });

  // Only log if there are actual changes
  const hasChanges = Object.values(changes).some(
    (type) => type.added.size > 0 || type.removed.size > 0
  );

  if (hasChanges) {
    const logParts = [];

    if (changes.weaponLead.added.size > 0) {
      logParts.push(
        `Weapon Lead Added: ${Array.from(changes.weaponLead.added.values())
          .map((r) => r.name)
          .join(", ")}`
      );
    }
    if (changes.weaponLead.removed.size > 0) {
      logParts.push(
        `Weapon Lead Removed: ${Array.from(changes.weaponLead.removed.values())
          .map((r) => r.name)
          .join(", ")}`
      );
    }
    if (changes.weapon.added.size > 0) {
      logParts.push(
        `Weapon Added: ${Array.from(changes.weapon.added.values())
          .map((r) => r.name)
          .join(", ")}`
      );
    }
    if (changes.weapon.removed.size > 0) {
      logParts.push(
        `Weapon Removed: ${Array.from(changes.weapon.removed.values())
          .map((r) => r.name)
          .join(", ")}`
      );
    }
    if (changes.guild.added.size > 0) {
      logParts.push(
        `Guild Added: ${Array.from(changes.guild.added.values())
          .map((r) => r.name)
          .join(", ")}`
      );
    }
    if (changes.guild.removed.size > 0) {
      logParts.push(
        `Guild Removed: ${Array.from(changes.guild.removed.values())
          .map((r) => r.name)
          .join(", ")}`
      );
    }

    if (logParts.length > 0) {
      Logger.info(`Role changes detected:\n${logParts.join("\n")}`);
    }
  }

  return changes;
};

const debouncedUpdateEmbed = debounce(
  async (guild, className, channelData) => {
    try {
      await updateClassChannelEmbed(guild, className, channelData);
    } catch (error) {
      Logger.error(`Failed to update embed for ${className}: ${error}`);
    }
  },
  2000,
  { maxWait: 5000 }
);

const debouncedHandleRoleUpdate = debounce(
  async (member, review) => {
    const weaponRoles = member.roles.cache.filter(
      (role) => WEAPON_TO_CLASS[role.id]
    );

    if (weaponRoles.size === 0) return;
    if (weaponRoles.size > 1) {
      Logger.info(
        `Waiting for role changes to settle - ${
          member.user.tag
        } has multiple weapon roles: ${Array.from(weaponRoles.values())
          .map((r) => r.name)
          .join(", ")}`
      );
      return;
    }

    const currentClass = getClassForWeapon(weaponRoles.first().id);
    const oldClass = review.className;

    // Handle thread rename if class hasn't changed
    if (currentClass === oldClass) {
      const thread = await member.guild.channels.cache
        .get(review.channelId)
        ?.threads.fetch(review.threadId)
        .catch(() => null);

      if (!thread) return;

      const wasRenamed = await handleThreadRename(
        thread,
        member.id,
        weaponRoles.first()
      );
      if (wasRenamed) {
        Logger.info(`Auto-renamed thread for ${member.user.tag}`);
        debouncedUpdateEmbed(
          member.guild,
          currentClass,
          REVIEW_CHANNELS[currentClass]
        );
      }
      return;
    }

    // Handle class change
    if (pendingUpdates.has(member.id)) return;

    pendingUpdates.add(member.id);
    try {
      const thread = await member.guild.channels.cache
        .get(review.channelId)
        ?.threads.fetch(review.threadId);
      if (thread) {
        await createThreadUpdateButton(
          thread,
          oldClass,
          currentClass,
          member.id
        );
      }
    } finally {
      setTimeout(() => pendingUpdates.delete(member.id), 5 * 60 * 1000);
    }
  },
  2000,
  { maxWait: 5000 }
);

client.on("guildMemberUpdate", async (oldMember, newMember) => {
  const changes = detectRoleChanges(oldMember, newMember);
  const hasChanges = [...Object.values(changes)].some(
    (type) => type.added.size > 0 || type.removed.size > 0
  );

  if (!hasChanges) return;

  const memberName =
    (await getIngameName(newMember.id)) || newMember.displayName;

  // Check if they gained a guild role
  if (changes.guild.added.size > 0) {
    await handleGuildRoleAddition(newMember);
  }

  // Handle weapon lead thread access
  await manageWeaponLeadThreadAccess(oldMember, newMember, memberName);

  // Handle class lead role management
  await manageClassLeadRole(newMember);

  // Remove the verbose role logging
  const review = reviewThreads.get(newMember.id);
  if (!review) {
    Logger.info(`No review found for ${memberName}`);
    return;
  }

  // Handle role updates with debounce
  await debouncedHandleRoleUpdate(newMember, review);

  // Update embed if guild roles changed
  if (changes.guild.added.size > 0 || changes.guild.removed.size > 0) {
    debouncedUpdateEmbed(
      newMember.guild,
      review.className,
      REVIEW_CHANNELS[review.className]
    );
  }
});

client.login(process.env.TOKEN);

const EMBED_COLORS = {
  DEFAULT: 0x2b2d31,
  SUCCESS: 0x57f287,
  WARNING: 0xfee75c,
  ERROR: 0xed4245,
};

const createClassChannelEmbed = async (guild, channelId, className) => {
  const channel = guild.channels.cache.get(channelId);
  if (!channel) return null;

  // Get all active reviews for this class
  const classReviews = Array.from(reviewThreads.entries()).filter(
    ([, review]) =>
      review.channelId === channelId && !review.archived && !review.locked
  );

  // Sort and prepare reviews
  const sortedReviews = await Promise.all(
    classReviews.map(async ([userId, review]) => {
      const thread = await channel.threads
        .fetch(review.threadId)
        .catch((error) => {
          Logger.error(`Failed to fetch thread ${review.threadId}: ${error}`);
          return null;
        });
      if (!thread) return null;

      const member = await guild.members.fetch(userId).catch(() => null);
      if (!member) return null;

      // Check if member has any guild roles
      const hasGuildRole = member.roles.cache.some((role) =>
        Object.values(GUILD_ROLES).some((guild) => guild.id === role.id)
      );

      // Get weapon role
      const weaponRole = member.roles.cache.find((role) =>
        REVIEW_CHANNELS[className].classRoleIds.includes(role.id)
      );

      const ingameName = await getIngameName(userId);
      // Skip this review if we failed to fetch from database
      if (ingameName === undefined) {
        Logger.warn(
          `Failed to fetch ingame name for ${userId} from database, skipping in embed`
        );
        return null;
      }

      const displayName = ingameName === null ? member.displayName : ingameName;

      return {
        userId,
        threadId: review.threadId,
        ingameName: displayName,
        weaponRole: weaponRole?.name || "Unknown",
        weaponRoleId: weaponRole?.id || "0",
        createdAt: thread.createdTimestamp,
        hasGuildRole,
      };
    })
  );

  const validReviews = sortedReviews
    .filter((review) => review !== null)
    .sort((a, b) => {
      // First sort by weapon role ID
      if (a.weaponRoleId !== b.weaponRoleId) {
        return a.weaponRoleId.localeCompare(b.weaponRoleId);
      }
      // Then sort by ingame name within the same weapon role
      return a.ingameName.localeCompare(b.ingameName, undefined, {
        sensitivity: "base",
      });
    });

  const baseEmbed = {
    color: EMBED_COLORS.DEFAULT,
    footer: {
      text: `Review List • ${className} • Last updated: ${new Date().toLocaleString()}`,
      iconURL: client.user.displayAvatarURL(),
    },
  };

  if (validReviews.length === 0) {
    return [
      {
        embeds: [
          {
            ...baseEmbed,
            title: `${
              className.charAt(0).toUpperCase() + className.slice(1)
            } Review Threads`,
            description: "No active review threads",
          },
        ],
      },
    ];
  }

  // Group reviews by weapon role and guild status
  const groupedReviews = validReviews.reduce((acc, review) => {
    if (!review.hasGuildRole) {
      if (!acc["No Guild"]) {
        acc["No Guild"] = [];
      }
      acc["No Guild"].push(review);
      return acc;
    }

    // Special handling for Unknown/Needs Migration
    if (review.weaponRole === "Unknown") {
      if (!acc["Needs Migration"]) {
        acc["Needs Migration"] = [];
      }
      acc["Needs Migration"].push(review);
      return acc;
    }

    if (!acc[review.weaponRole]) {
      acc[review.weaponRole] = [];
    }
    acc[review.weaponRole].push(review);
    return acc;
  }, {});

  // Function to split a category if it's too large
  const splitCategory = (categoryName, reviews, maxSize = 2000) => {
    const sections = [];
    let currentSection = [];
    let currentSize = 0;
    const headerSize = `**${categoryName}**\n`.length;

    for (const review of reviews) {
      const reviewLine = `• [${review.ingameName}](https://discord.com/channels/${guild.id}/${review.threadId})\n`;
      const lineSize = reviewLine.length;

      // If adding this line would exceed maxSize, start a new section
      if (
        currentSize + lineSize + headerSize > maxSize &&
        currentSection.length > 0
      ) {
        sections.push(currentSection);
        currentSection = [];
        currentSize = 0;
      }

      currentSection.push(review);
      currentSize += lineSize;
    }

    // Add remaining reviews
    if (currentSection.length > 0) {
      sections.push(currentSection);
    }

    return sections.map((sectionReviews, index, array) => ({
      name:
        array.length > 1
          ? `${categoryName} (${index + 1}/${array.length})`
          : categoryName,
      content: sectionReviews,
    }));
  };

  // Process each category and split if necessary
  const processedSections = [];
  const groupEntries = Object.entries(groupedReviews).filter(
    ([key]) => key !== "No Guild" && key !== "Needs Migration"
  );

  for (const [weaponRole, reviews] of groupEntries) {
    const splitSections = splitCategory(weaponRole, reviews);
    processedSections.push(...splitSections);
  }

  // Handle No Guild section
  if (groupedReviews["No Guild"]?.length > 0) {
    const noGuildSections = splitCategory(
      "No Longer In Guild",
      groupedReviews["No Guild"]
    );
    processedSections.push(...noGuildSections);
  }

  // Handle Needs Migration section last
  if (groupedReviews["Needs Migration"]?.length > 0) {
    const needsMigrationSections = splitCategory(
      "Needs Migration",
      groupedReviews["Needs Migration"]
    );
    processedSections.push(...needsMigrationSections);
  }

  // Create embeds with the processed sections
  const embeds = [];
  let currentEmbed = {
    description: "",
    sections: [],
    size: 0,
  };

  for (const section of processedSections) {
    const sectionContent = `**${section.name}**\n${section.content
      .map(
        (review) =>
          `• [${review.ingameName}](https://discord.com/channels/${guild.id}/${review.threadId})`
      )
      .join("\n")}\n\n`;

    const sectionSize = sectionContent.length;

    // If adding this section would exceed embed limit, create a new embed
    if (currentEmbed.size + sectionSize > 4000) {
      embeds.push(currentEmbed);
      currentEmbed = {
        description: "",
        sections: [],
        size: 0,
      };
    }

    currentEmbed.description += sectionContent;
    currentEmbed.sections.push(section);
    currentEmbed.size += sectionSize;
  }

  // Add the last embed if it has content
  if (currentEmbed.size > 0) {
    embeds.push(currentEmbed);
  }

  // Format the final embeds
  return embeds.map((embed, index) => ({
    embeds: [
      {
        ...baseEmbed,
        title:
          embeds.length > 1
            ? `${
                className.charAt(0).toUpperCase() + className.slice(1)
              } Review Threads (${index + 1}/${embeds.length})`
            : `${
                className.charAt(0).toUpperCase() + className.slice(1)
              } Review Threads`,
        description: embed.description.trim(),
        footer: {
          text:
            embeds.length > 1
              ? `Review List • ${className} • Part ${index + 1}/${
                  embeds.length
                } • Last updated: ${new Date().toLocaleString()}`
              : `Review List • ${className} • Last updated: ${new Date().toLocaleString()}`,
          iconURL: client.user.displayAvatarURL(),
        },
      },
    ],
  }));
};

const updateClassChannelEmbed = async (guild, className, channelData) => {
  let retryCount = 0;
  while (retryCount < 3) {
    try {
      const channel = guild.channels.cache.get(channelData.channelId);
      if (!channel) return;

      const messages = await retryOperation(async () =>
        channel.messages.fetch({ limit: 20 })
      );

      const existingEmbeds = messages
        .filter(
          (msg) =>
            msg.author.id === client.user.id &&
            (msg.embeds[0]?.footer?.text?.startsWith(
              `Review List • ${className}`
            ) ||
              msg.embeds[0]?.title?.includes(`${className} Review Threads`))
        )
        .sort((a, b) => b.createdTimestamp - a.createdTimestamp);

      const newEmbeds = await createClassChannelEmbed(
        guild,
        channelData.channelId,
        className
      );
      if (!newEmbeds) return;

      // Compare existing embeds with new ones
      let needsUpdate = false;
      if (existingEmbeds.size !== newEmbeds.length) {
        needsUpdate = true;
      } else {
        for (let i = 0; i < newEmbeds.length; i++) {
          const existingEmbed = existingEmbeds.at(i)?.embeds[0];
          const newEmbed = newEmbeds[i].embeds[0];

          if (
            !existingEmbed ||
            existingEmbed.description !== newEmbed.description ||
            existingEmbed.title !== newEmbed.title
          ) {
            needsUpdate = true;
            break;
          }
        }
      }

      if (!needsUpdate) {
        Logger.info(`Skipping ${className} embed update - content unchanged`);
        return;
      }

      // Delete old embeds with retries
      if (existingEmbeds.size > 0) {
        await Promise.all(
          existingEmbeds.map((msg) =>
            retryOperation(() => msg.delete()).catch((error) => {
              Logger.error(
                `Error deleting old embed for ${className}: ${error}`
              );
            })
          )
        );
      }

      // Send new embeds with retries
      for (const embedData of newEmbeds) {
        await retryOperation(async () => {
          await channel.send(embedData);
        }, 3);
      }
      Logger.info(`Updated ${className} embed with new content`);
      break;
    } catch (error) {
      retryCount++;
      if (retryCount === 3) {
        Logger.error(
          `Failed to update embed after ${retryCount} attempts: ${error}`
        );
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 1000 * retryCount));
    }
  }
};

const validateReviewWeaponRoles = async (guild) => {
  const updates = [];
  const channelsToUpdate = new Set();

  for (const [userId, review] of reviewThreads.entries()) {
    try {
      const member = await guild.members.fetch(userId).catch(() => null);
      if (!member) continue;

      // Check for guild roles
      const hasGuildRole = member.roles.cache.some((role) =>
        Object.values(GUILD_ROLES).some((guild) => guild.id === role.id)
      );

      const weaponRoles = member.roles.cache.filter(
        (role) => WEAPON_TO_CLASS[role.id]
      );

      // Add channel to update if member has no guild role
      if (!hasGuildRole) {
        channelsToUpdate.add(review.className);
      }

      if (weaponRoles.size === 0) continue;

      if (weaponRoles.size > 1) {
        Logger.info(`Multiple weapon roles detected for ${member.user.tag}`);
      }

      const currentClass = getClassForWeapon(weaponRoles.first().id);
      if (currentClass !== review.className) {
        updates.push({ member, review, currentClass });
        channelsToUpdate.add(review.className);
        channelsToUpdate.add(currentClass);
      }
    } catch {
      continue; // Skip failed validations
    }
  }

  if (updates.length > 0 || channelsToUpdate.size > 0) {
    for (const { member, review, currentClass } of updates) {
      try {
        const channel = await guild.channels.fetch(review.channelId);
        const thread = await channel.threads
          .fetch(review.threadId)
          .catch(() => null);
        if (!thread) {
          reviewThreads.delete(member.id);
          continue;
        }

        const messages = await thread.messages.fetch({ limit: 10 });
        const hasUpdateButton = messages.some(
          (msg) =>
            msg.author.id === client.user.id &&
            msg.components?.length > 0 &&
            msg.components[0].components?.some((component) =>
              component.customId?.startsWith("update_thread_")
            )
        );

        if (!hasUpdateButton) {
          await createThreadUpdateButton(
            thread,
            review.className,
            currentClass,
            member.id
          );
        }
      } catch (error) {
        Logger.error(
          `Failed to process update for ${member.user.tag}: ${error}`
        );
      }
    }

    // Update embeds for all affected channels
    for (const className of channelsToUpdate) {
      try {
        await updateClassChannelEmbed(
          guild,
          className,
          REVIEW_CHANNELS[className]
        );
      } catch {
        continue; // Skip failed embed updates
      }
    }
  }
};

const updateReviewState = async (userId, updates) => {
  const oldState = reviewThreads.get(userId);
  if (!oldState) {
    Logger.error(`No existing review state found for ${userId}`);
    return null;
  }

  const newState = { ...oldState, ...updates };
  reviewThreads.set(userId, newState);
  Logger.info(`Updated review state for ${userId}`);
  return newState;
};

process.on("SIGINT", async () => {
  Logger.info("Received SIGINT. Cleaning up...");
  try {
    // Clear all state
    pendingUpdates.clear();
    migratingThreads.clear();

    // Cancel any pending debounced operations
    debouncedUpdateEmbed.cancel();
    debouncedHandleRoleUpdate.cancel();

    // Gracefully destroy the client
    await client.destroy();

    process.exit(0);
  } catch (error) {
    Logger.error("Error during cleanup:", error);
    process.exit(1);
  }
});

async function handleReviewMigrationCreation(
  guild,
  userId,
  [className, classData],
  channel,
  ingameName
) {
  Logger.info(
    `Starting thread creation for ${ingameName} (${userId}) in ${className}`
  );

  const userMember = await guild.members.fetch(userId);
  const classRole = classData.classRoleIds
    .map((roleId) => guild.roles.cache.get(roleId))
    .find((role) => userMember.roles.cache.has(role?.id));

  if (!classRole) {
    Logger.warn(`No matching class role found for ${ingameName} (${userId})`);
    return null;
  }

  // Create thread
  Logger.info(`Creating thread for ${ingameName} with role ${classRole.name}`);
  const thread = await channel.threads.create({
    name: formatThreadName(ingameName, classRole.name, userId),
    autoArchiveDuration: 10080,
    type: 12,
  });
  Logger.info(`Created thread ${thread.id} for ${ingameName}`);

  // Add close button
  const row = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`close_review_${userId}`)
      .setLabel("Close Review")
      .setStyle(ButtonStyle.Danger)
  );
  await thread.send({
    content: "Click the button below to close this review thread:",
    components: [row],
  });
  Logger.info(`Added close button to thread ${thread.id}`);

  return thread;
}

const validateThreadNames = async (guild) => {
  Logger.info("Starting thread name validation...");

  for (const [userId, review] of reviewThreads.entries()) {
    try {
      const member = await guild.members.fetch(userId).catch(() => null);
      if (!member) continue;

      const weaponRoles = member.roles.cache.filter(
        (role) => WEAPON_TO_CLASS[role.id]
      );
      if (weaponRoles.size !== 1) continue;

      const currentClass = getClassForWeapon(weaponRoles.first().id);
      const thread = await guild.channels.cache
        .get(review.channelId)
        ?.threads.fetch(review.threadId)
        .catch(() => null);

      if (!thread) {
        reviewThreads.delete(userId);
        continue;
      }

      // Skip archived threads
      if (thread.archived || thread.locked) {
        continue;
      }

      // Check if thread needs class migration
      if (currentClass !== review.className) {
        if (!pendingUpdates.has(userId)) {
          // Check for existing update button
          const messages = await thread.messages.fetch({ limit: 10 });
          const hasUpdateButton = messages.some(
            (msg) =>
              msg.author.id === client.user.id &&
              msg.components?.length > 0 &&
              msg.components[0].components?.some((component) =>
                component.customId?.startsWith("update_thread_")
              )
          );

          if (!hasUpdateButton) {
            pendingUpdates.add(userId);
            await createThreadUpdateButton(
              thread,
              review.className,
              currentClass,
              userId
            );
            setTimeout(() => pendingUpdates.delete(userId), 5 * 60 * 1000);
          }
        }
        continue;
      }

      // Check if thread name needs update
      const wasRenamed = await handleThreadRename(
        thread,
        userId,
        weaponRoles.first()
      );
      if (wasRenamed) {
        Logger.info(
          `Auto-renamed thread for ${member.user.tag} during validation`
        );
        debouncedUpdateEmbed(
          guild,
          currentClass,
          REVIEW_CHANNELS[currentClass]
        );
      }
    } catch (error) {
      Logger.error(`Error validating thread for ${userId}: ${error}`);
    }
  }
  Logger.info("Thread name validation complete");
};

const cleanupDuplicateMigrationMessages = async (guild) => {
  Logger.info("Starting cleanup of duplicate migration messages...");
  Logger.info(`Checking ${reviewThreads.size} review threads for duplicates`);

  let checkedThreads = 0;
  let cleanedThreads = 0;
  let totalDuplicates = 0;

  for (const [userId, review] of reviewThreads.entries()) {
    try {
      checkedThreads++;
      const thread = await guild.channels.cache
        .get(review.channelId)
        ?.threads.fetch(review.threadId)
        .catch(() => null);

      if (!thread) {
        Logger.info(`Thread not found for user ${userId}`);
        continue;
      }

      if (thread.archived || thread.locked) {
        Logger.info(`Skipping archived/locked thread ${thread.name}`);
        continue;
      }

      Logger.info(`Checking thread ${thread.name} for duplicate messages...`);

      // Fetch all messages recursively
      let allMessages = new Map();
      let lastId = null;

      while (true) {
        const options = { limit: 100 };
        if (lastId) {
          options.before = lastId;
        }

        const messages = await thread.messages.fetch(options);
        if (messages.size === 0) break;

        messages.forEach((msg) => allMessages.set(msg.id, msg));
        lastId = messages.last().id;
      }

      Logger.info(
        `Found ${allMessages.size} total messages in thread ${thread.name}`
      );

      const migrationMessages = Array.from(allMessages.values())
        .filter(
          (msg) =>
            msg.author.id === client.user.id &&
            msg.components?.length > 0 &&
            msg.components[0].components?.some((component) =>
              component.customId?.startsWith("update_thread_")
            )
        )
        .sort((a, b) => b.createdTimestamp - a.createdTimestamp); // newest first

      if (migrationMessages.length > 1) {
        cleanedThreads++;
        const toDelete = migrationMessages.slice(1);
        totalDuplicates += toDelete.length;
        Logger.info(
          `Found ${migrationMessages.length} migration messages in thread ${thread.name}, cleaning up ${toDelete.length}...`
        );

        // Delete in batches to avoid rate limits
        const batchSize = 5;
        for (let i = 0; i < toDelete.length; i += batchSize) {
          const batch = toDelete.slice(i, i + batchSize);
          await Promise.all(batch.map((msg) => msg.delete().catch(() => {})));
          if (i + batchSize < toDelete.length) {
            await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait 1s between batches
          }
        }

        Logger.info(`Finished cleaning up thread ${thread.name}`);
      } else {
        Logger.info(`No duplicate messages found in thread ${thread.name}`);
      }
    } catch (error) {
      Logger.error(
        `Error cleaning up migration messages for ${userId}: ${error}`
      );
    }
  }

  Logger.info(
    `Duplicate migration message cleanup complete:\n` +
      `- Checked ${checkedThreads} threads\n` +
      `- Cleaned ${cleanedThreads} threads\n` +
      `- Removed ${totalDuplicates} duplicate messages`
  );
};

// Helper function to get all weapon lead roles for a class
const getWeaponLeadRolesForClass = (className) => {
  const channelData = REVIEW_CHANNELS[className];
  return channelData ? Object.values(channelData.weaponLeadRoleIds) : [];
};

// Helper function to check if a member has any weapon lead roles for a class
const hasWeaponLeadRolesForClass = (member, className) => {
  const weaponLeadRoles = getWeaponLeadRolesForClass(className);
  return member.roles.cache.some((role) => weaponLeadRoles.includes(role.id));
};

// Function to manage class lead roles based on weapon lead roles
const manageClassLeadRole = async (member) => {
  try {
    let hasAnyClassLead = false; // Track if they have any class lead role

    // First handle individual class leads
    for (const [className, channelData] of Object.entries(REVIEW_CHANNELS)) {
      if (!channelData.weaponLeadRoleIds) continue;

      const weaponLeadRoleIds = Object.values(channelData.weaponLeadRoleIds);
      const hasAnyWeaponLead = weaponLeadRoleIds.some((roleId) =>
        member.roles.cache.has(roleId)
      );
      const hasClassLead = member.roles.cache.has(channelData.leadRoleId);

      if (hasAnyWeaponLead && !hasClassLead) {
        await member.roles.add(channelData.leadRoleId);
        Logger.info(
          `Added ${className} lead role to ${
            member.user.tag
          } (has ${member.roles.cache
            .filter((role) => weaponLeadRoleIds.includes(role.id))
            .map((role) => role.name)
            .join(", ")})`
        );
      } else if (!hasAnyWeaponLead && hasClassLead) {
        await member.roles.remove(
          channelData.leadRoleId,
          "Lost all weapon lead roles"
        );
        Logger.info(
          `Removed ${className} lead role from ${member.user.tag} (lost all weapon lead roles)`
        );
      }

      // Update hasAnyClassLead if they have this class lead
      if (hasAnyWeaponLead || hasClassLead) {
        hasAnyClassLead = true;
      }
    }

    // Now handle master lead role
    const hasMasterLead = member.roles.cache.has(MASTER_LEAD_ROLE_ID);

    if (hasAnyClassLead && !hasMasterLead) {
      await member.roles.add(MASTER_LEAD_ROLE_ID);
      Logger.info(
        `Added master lead role to ${member.user.tag} (has class lead role(s))`
      );
    } else if (!hasAnyClassLead && hasMasterLead) {
      await member.roles.remove(
        MASTER_LEAD_ROLE_ID,
        "Lost all class lead roles"
      );
      Logger.info(
        `Removed master lead role from ${member.user.tag} (lost all class lead roles)`
      );
    }
  } catch (error) {
    Logger.error(
      `Error managing class lead roles for ${member.user.tag}: ${error.message}`
    );
  }
};

// Helper function to get weapon role changes for a class
const getWeaponRoleChanges = (oldMember, newMember, weaponLeadRoleIds) => {
  const added = newMember.roles.cache.filter(
    (role) =>
      weaponLeadRoleIds.includes(role.id) && !oldMember.roles.cache.has(role.id)
  );
  const removed = oldMember.roles.cache.filter(
    (role) =>
      weaponLeadRoleIds.includes(role.id) && !newMember.roles.cache.has(role.id)
  );
  return { added, removed };
};

// Function to manage weapon lead thread access
const manageWeaponLeadThreadAccess = async (
  oldMember,
  newMember,
  memberName
) => {
  try {
    for (const [className, channelData] of Object.entries(REVIEW_CHANNELS)) {
      if (!channelData.weaponLeadRoleIds) continue;

      for (const [weaponRoleId, weaponLeadRoleId] of Object.entries(
        channelData.weaponLeadRoleIds
      )) {
        const hadRole = oldMember.roles.cache.has(weaponLeadRoleId);
        const hasRole = newMember.roles.cache.has(weaponLeadRoleId);

        if (hadRole === hasRole) continue;

        const relevantThreads = Array.from(reviewThreads.entries()).filter(
          ([userId, review]) => {
            if (
              review.className !== className ||
              review.archived ||
              review.locked
            )
              return false;
            const threadOwner = newMember.guild.members.cache.get(userId);
            return threadOwner?.roles.cache.has(weaponRoleId);
          }
        );

        // Get the role name
        const weaponLeadRole =
          newMember.guild.roles.cache.get(weaponLeadRoleId);
        Logger.info(
          `${memberName} ${hasRole ? "gained" : "lost"} weapon lead role ` +
            `${weaponLeadRole?.name || weaponLeadRoleId} ` +
            `(${relevantThreads.length} relevant threads)`
        );

        for (const [userId, review] of relevantThreads) {
          try {
            if (userId === newMember.id) continue;

            const channel = await newMember.guild.channels.fetch(
              review.channelId
            );
            const thread = await channel.threads.fetch(review.threadId);

            const threadMembers = await thread.members.fetch();
            const isMember = threadMembers.has(newMember.id);
            const threadMember = await thread.members
              .fetch(newMember.id)
              .catch(() => null);

            if (!hasRole && (isMember || threadMember)) {
              await safeThreadMemberUpdate(
                thread,
                newMember.id,
                "remove",
                newMember,
                `Lost weapon lead role for ${className}`
              );
            } else if (hasRole && !isMember && !threadMember) {
              await safeThreadMemberUpdate(
                thread,
                newMember.id,
                "add",
                newMember,
                `Gained weapon lead role for ${className}`
              );
            }
          } catch (error) {
            Logger.error(
              `Error processing thread ${review.threadId} for ${memberName}: ${error.message}`
            );
          }
        }
      }
    }
  } catch (error) {
    Logger.error(
      `Error managing weapon lead thread access for ${memberName}: ${error.message}`
    );
  }
};

// Add this helper function
const safeThreadMemberUpdate = async (
  thread,
  memberId,
  action,
  member,
  reason
) => {
  try {
    if (action === "add") {
      await thread.members.add(memberId);
      logThreadMemberChange("Added", member, thread, reason);
    } else if (action === "remove") {
      await thread.members.remove(memberId);
      logThreadMemberChange("Removed", member, thread, reason);
    }
  } catch (error) {
    Logger.error(
      `Failed to ${action} member ${memberId} to thread ${thread.id}: ${error.message}`
    );
    throw error; // Re-throw to let caller handle
  }
};

const handleGuildRoleAddition = async (member) => {
  const userId = member.id;
  const review = reviewThreads.get(userId);

  if (review) {
    try {
      const channel = await member.guild.channels.cache.get(review.channelId);
      const thread = await channel?.threads
        .fetch(review.threadId)
        .catch(() => null);

      if (thread) {
        // Re-add member to thread
        await thread.members.add(userId);

        // If thread was archived/locked, reopen it
        if (thread.archived || thread.locked) {
          if (thread.archived) await thread.setArchived(false);
          if (thread.locked) await thread.setLocked(false);

          // Sync thread members after reopening
          await syncThreadMembers(thread, userId, review.className);

          reviewThreads.set(userId, {
            ...review,
            archived: false,
            locked: false,
          });

          await thread.send({
            content: `Thread reopened automatically - ${member.user.tag} has rejoined the guild.`,
            allowedMentions: { parse: [] },
          });

          // Update the embed
          debouncedUpdateEmbed(
            member.guild,
            review.className,
            REVIEW_CHANNELS[review.className]
          );
        }

        Logger.info(
          `Auto-readded ${member.user.tag} to their review thread after guild role addition`
        );
      }
    } catch (error) {
      Logger.error(
        `Error handling guild role addition for ${member.user.tag}: ${error}`
      );
    }
  }
};

// New helper function to sync thread members
const syncThreadMembers = async (thread, userId, className) => {
  try {
    // Get all current thread members
    const threadMembers = await thread.members.fetch();
    const guild = thread.guild;

    // Remove everyone except the thread owner
    await Promise.all(
      Array.from(threadMembers.values())
        .filter((threadMember) => threadMember.id !== userId)
        .map((threadMember) =>
          safeThreadMemberUpdate(
            thread,
            threadMember.id,
            "remove",
            threadMember.guildMember,
            "Thread reopened - resetting members"
          )
        )
    );

    // Get the thread owner and ensure they're added first
    const threadOwner = await guild.members.fetch(userId);
    await safeThreadMemberUpdate(
      thread,
      userId,
      "add",
      threadOwner,
      "Thread reopened - ensuring owner access"
    );

    // Rest of the weapon lead logic...
    const weaponRole = threadOwner.roles.cache.find((role) =>
      REVIEW_CHANNELS[className].classRoleIds.includes(role.id)
    );

    if (weaponRole) {
      // Get and add the appropriate weapon leads
      const weaponLeadRoleId =
        REVIEW_CHANNELS[className].weaponLeadRoleIds?.[weaponRole.id];
      if (weaponLeadRoleId) {
        const weaponLeads = (await guild.members.fetch()).filter((m) =>
          m.roles.cache.has(weaponLeadRoleId)
        );

        // Add weapon leads
        await Promise.all(
          Array.from(weaponLeads.values())
            .filter((lead) => lead.id !== userId) // Don't add the owner twice
            .map((lead) =>
              safeThreadMemberUpdate(
                thread,
                lead.id,
                "add",
                lead,
                `Thread reopened - adding weapon lead for ${className}`
              )
            )
        );
      }
    }

    Logger.info(`Synced members for thread ${thread.name} after reopening`);
  } catch (error) {
    Logger.error(`Error syncing thread members for ${thread.name}: ${error}`);
    throw error;
  }
};
