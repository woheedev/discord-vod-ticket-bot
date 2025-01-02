import { Client, Databases, Query } from "node-appwrite";

// Initialize Appwrite client
const client = new Client()
  .setEndpoint(process.env.APPWRITE_ENDPOINT)
  .setProject(process.env.APPWRITE_PROJECT_ID)
  .setKey(process.env.APPWRITE_API_KEY);

const databases = new Databases(client);

// Cache for ingame names
const ingameNameCache = new Map();
const CACHE_DURATION = 30 * 60 * 1000; // 30 minutes

export async function initializeDb() {
  try {
    // Check if we can connect to the database
    await databases.get(process.env.APPWRITE_DATABASE_ID);
    console.log("Successfully connected to Appwrite database");
  } catch (error) {
    console.error("Failed to connect to Appwrite database:", error);
    throw error;
  }
}

export const getIngameName = async (discordId, retries = 3) => {
  // Check cache first
  const cached = ingameNameCache.get(discordId);
  if (cached && Date.now() - cached.timestamp < CACHE_DURATION) {
    return cached.name;
  }

  for (let i = 0; i < retries; i++) {
    try {
      const response = await databases.listDocuments(
        process.env.APPWRITE_DATABASE_ID,
        process.env.APPWRITE_COLLECTION_ID,
        [Query.equal("discord_id", discordId)]
      );

      const ingameName =
        response.documents.length > 0
          ? response.documents[0].ingame_name
          : null;

      // Cache the result (even if null)
      ingameNameCache.set(discordId, {
        name: ingameName,
        timestamp: Date.now(),
      });

      return ingameName;
    } catch (error) {
      if (i === retries - 1) {
        console.error(
          `Failed to get ingame name for user ${discordId}: ${error}`
        );
        return null;
      }
      // Wait before retrying, with exponential backoff
      await new Promise((resolve) =>
        setTimeout(resolve, Math.pow(2, i) * 1000)
      );
      console.warn(
        `Retrying getIngameName for ${discordId}, attempt ${i + 2}/${retries}`
      );
    }
  }
  return null;
};

// Function to pre-cache multiple ingame names at once
export const bulkGetIngameNames = async (discordIds) => {
  try {
    const response = await databases.listDocuments(
      process.env.APPWRITE_DATABASE_ID,
      process.env.APPWRITE_COLLECTION_ID,
      [Query.equal("discord_id", discordIds)]
    );

    response.documents.forEach((doc) => {
      ingameNameCache.set(doc.discord_id, {
        name: doc.ingame_name,
        timestamp: Date.now(),
      });
    });

    return response.documents;
  } catch (error) {
    console.error(`Failed to bulk fetch ingame names: ${error}`);
    return [];
  }
};

// Function to clear old cache entries
export const cleanCache = () => {
  const now = Date.now();
  for (const [key, value] of ingameNameCache.entries()) {
    if (now - value.timestamp > CACHE_DURATION) {
      ingameNameCache.delete(key);
    }
  }
};
