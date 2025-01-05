import { Client, Databases, Query } from "node-appwrite";
import { Logger } from "./logger.js";

// Initialize Appwrite client
const client = new Client()
  .setEndpoint(process.env.APPWRITE_ENDPOINT)
  .setProject(process.env.APPWRITE_PROJECT_ID)
  .setKey(process.env.APPWRITE_API_KEY);

const databases = new Databases(client);

export async function initializeDb() {
  try {
    // Check if we can connect to the database
    await databases.get(process.env.APPWRITE_DATABASE_ID);
    Logger.info("Successfully connected to Appwrite database");
  } catch (error) {
    Logger.error("Failed to connect to Appwrite database:", error);
    throw error;
  }
}

export const getIngameName = async (discordId, retries = 3) => {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await databases.listDocuments(
        process.env.APPWRITE_DATABASE_ID,
        process.env.APPWRITE_COLLECTION_ID,
        [Query.equal("discord_id", discordId)]
      );

      if (response.documents.length > 0) {
        return response.documents[0].ingame_name;
      }
      return null;
    } catch (error) {
      if (i === retries - 1) {
        Logger.error(
          `Failed to get ingame name for user ${discordId}: ${error}`
        );
        return null;
      }
      // Wait before retrying, with exponential backoff
      await new Promise((resolve) =>
        setTimeout(resolve, Math.pow(2, i) * 1000)
      );
      Logger.warn(
        `Retrying getIngameName for ${discordId}, attempt ${i + 2}/${retries}`
      );
    }
  }
  return null;
};
