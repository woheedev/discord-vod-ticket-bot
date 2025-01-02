import { Logger } from "./logger.js";

export const retryOperation = async (operation, maxRetries = 3) => {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await operation();
    } catch (error) {
      if (i === maxRetries - 1) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1000 * (i + 1)));
      Logger.warn(`Retrying operation, attempt ${i + 2}/${maxRetries}`);
    }
  }
};

export const validateThreadAccess = async (
  userId,
  interaction,
  isAdmin = false
) => {
  if (!isAdmin && interaction.user.id !== userId) {
    throw new Error("You can only update your own thread.");
  }

  const review = reviewThreads.get(userId);
  if (!review || review.archived || review.locked) {
    throw new Error("No active review thread found.");
  }

  return review;
};

export const isReviewChannel = (channelId) => {
  return Object.values(REVIEW_CHANNELS).some(
    (channel) => channel.channelId === channelId
  );
};
