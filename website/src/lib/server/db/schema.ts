import { sqliteTable, text } from 'drizzle-orm/sqlite-core';

export const contentBlocks = sqliteTable('content_blocks', {
	key: text('key').primaryKey(),
	value: text('value').notNull(),
	updatedAt: text('updated_at')
		.notNull()
		.$defaultFn(() => new Date().toISOString())
});
