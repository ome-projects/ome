# OME website

The redesigned OME documentation site, served at `lightseek.org/ome` from
launch. Documentation changes go to `src/lib/content/`; `../site/` is retained
for migration reference.

## Develop

You need Node 22 or newer and pnpm 10.

```bash
pnpm install
pnpm dev
```

The site runs at http://localhost:5173/ome.

## Check

```bash
pnpm lint && pnpm check && pnpm test && pnpm build
```

Pages live in `src/lib/content/`, and `src/lib/config/nav.ts` orders them.
The Writing docs page, `src/lib/content/contributing/writing-docs.md`,
covers the syntax and style.

The layout and styles come from
[smg-docs](https://github.com/smg-project/smg-docs).
