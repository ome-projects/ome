// See https://svelte.dev/docs/kit/types#app.d.ts
// for information about these interfaces
declare global {
	// `wrangler types` only emits secret bindings when a local .dev.vars file
	// exists; merge the declaration so CI-regenerated types stay complete.
	interface Env {
		GITHUB_TOKEN: string;
	}

	namespace App {
		interface Platform {
			env: Env;
			ctx: ExecutionContext;
			// The DOM lib's CacheStorage hides the Workers-only `default` cache.
			caches: CacheStorage & { readonly default: Cache };
			cf?: IncomingRequestCfProperties;
		}

		// interface Error {}
		// interface Locals {}
		// interface PageData {}
		// interface PageState {}
	}
}

export {};
