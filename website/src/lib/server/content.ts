import { inArray } from 'drizzle-orm';
import { getDb } from '$lib/server/db';
import { contentBlocks } from '$lib/server/db/schema';

export type ContentMap = Record<string, string>;

export async function getContentBlocks(
	db: D1Database,
	keys: string[],
	fallbacks: ContentMap = {}
): Promise<ContentMap> {
	if (keys.length === 0) return { ...fallbacks };

	try {
		const drizzle = getDb(db);
		const rows = await drizzle.select().from(contentBlocks).where(inArray(contentBlocks.key, keys));

		const result: ContentMap = { ...fallbacks };
		for (const row of rows) {
			result[row.key] = row.value;
		}

		return result;
	} catch {
		return { ...fallbacks };
	}
}
