import { defineConfig } from 'drizzle-kit';

const accountId = process.env.CLOUDFLARE_ACCOUNT_ID;
const databaseId = process.env.CLOUDFLARE_DATABASE_ID;
const token = process.env.CLOUDFLARE_D1_TOKEN;
const hasRemoteCredentials = Boolean(accountId && databaseId && token);

export default defineConfig({
	schema: './src/lib/server/db/schema.ts',
	out: './drizzle',
	dialect: 'sqlite',
	...(hasRemoteCredentials
		? {
				driver: 'd1-http',
				dbCredentials: {
					accountId: accountId!,
					databaseId: databaseId!,
					token: token!
				}
			}
		: {})
});
