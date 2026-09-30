import { execSync, spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';

const REQUIRED_MAJOR = 22;
const nodeVersion = process.versions.node;
const major = Number.parseInt(nodeVersion.split('.')[0], 10);

const commands = {
	build: 'wrangler types && vite build',
	preview: 'wrangler pages dev .svelte-kit/cloudflare --port 4173',
	check: 'wrangler types && svelte-kit sync && svelte-check --tsconfig ./tsconfig.json',
	gen: 'wrangler types',
	'db:migrate:local': 'wrangler d1 migrations apply ome-db --local'
};

const commandName = process.argv[2];

if (!commandName || !commands[commandName]) {
	console.error(`Unknown command: ${commandName ?? '(missing)'}`);
	process.exit(1);
}

function readRequiredVersion() {
	try {
		return readFileSync('.node-version', 'utf8').trim();
	} catch {
		return String(REQUIRED_MAJOR);
	}
}

function runViaFnm(version, command) {
	execSync(`fnm install ${version}`, { stdio: 'inherit' });
	const result = spawnSync('fnm', ['exec', '--using', version, '--', 'sh', '-c', command], {
		stdio: 'inherit'
	});

	if (result.status !== 0) {
		process.exit(result.status ?? 1);
	}
}

function printHelp(version) {
	console.error(`
Node ${nodeVersion} is installed, but wrangler needs Node ${REQUIRED_MAJOR}+.

Quick fix:
  pnpm node:use
  # or
  fnm install ${version} && fnm use ${version}

Optional (auto-switch on cd):
  eval "$(fnm env --use-on-cd)"   # add to ~/.zshrc
`);
}

const requiredVersion = readRequiredVersion();

if (major >= REQUIRED_MAJOR) {
	execSync(commands[commandName], { stdio: 'inherit', shell: true });
	process.exit(0);
}

try {
	execSync('fnm --version', { stdio: 'ignore' });
} catch {
	printHelp(requiredVersion);
	process.exit(1);
}

try {
	runViaFnm(requiredVersion, commands[commandName]);
} catch {
	printHelp(requiredVersion);
	process.exit(1);
}
