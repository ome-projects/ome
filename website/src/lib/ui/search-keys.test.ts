import { describe, expect, it } from 'vitest';
import { isPlainClick, isSearchShortcut, moveSelection, shortcutLabel } from './search-keys';

const key = (init: Partial<KeyboardEvent>) => ({
	key: 'k',
	metaKey: false,
	ctrlKey: false,
	altKey: false,
	shiftKey: false,
	...init
});

describe('isSearchShortcut', () => {
	it('accepts ⌘K and Ctrl+K', () => {
		expect(isSearchShortcut(key({ metaKey: true }))).toBe(true);
		expect(isSearchShortcut(key({ ctrlKey: true }))).toBe(true);
		expect(isSearchShortcut(key({ key: 'K', metaKey: true }))).toBe(true);
	});

	it('rejects other keys and modifiers', () => {
		expect(isSearchShortcut(key({}))).toBe(false);
		expect(isSearchShortcut(key({ key: 'j', metaKey: true }))).toBe(false);
		expect(isSearchShortcut(key({ metaKey: true, shiftKey: true }))).toBe(false);
		expect(isSearchShortcut(key({ ctrlKey: true, altKey: true }))).toBe(false);
	});
});

describe('moveSelection', () => {
	it('moves and wraps', () => {
		expect(moveSelection(0, 1, 3)).toBe(1);
		expect(moveSelection(2, 1, 3)).toBe(0);
		expect(moveSelection(0, -1, 3)).toBe(2);
	});

	it('starts from either end without a selection', () => {
		expect(moveSelection(-1, 1, 3)).toBe(0);
		expect(moveSelection(-1, -1, 3)).toBe(2);
	});

	it('returns -1 without items', () => {
		expect(moveSelection(0, 1, 0)).toBe(-1);
	});
});

describe('shortcutLabel', () => {
	it('uses ⌘ on Apple platforms and Ctrl elsewhere', () => {
		expect(shortcutLabel('MacIntel')).toBe('⌘K');
		expect(shortcutLabel('iPhone')).toBe('⌘K');
		expect(shortcutLabel('Win32')).toBe('Ctrl K');
		expect(shortcutLabel('Linux x86_64')).toBe('Ctrl K');
		expect(shortcutLabel('')).toBe('Ctrl K');
	});
});

describe('isPlainClick', () => {
	const click = (init: Partial<MouseEvent>) => ({
		button: 0,
		metaKey: false,
		ctrlKey: false,
		altKey: false,
		shiftKey: false,
		...init
	});

	it('accepts a plain left click', () => {
		expect(isPlainClick(click({}))).toBe(true);
	});

	it('leaves other buttons and modified clicks to the browser', () => {
		expect(isPlainClick(click({ button: 1 }))).toBe(false);
		expect(isPlainClick(click({ metaKey: true }))).toBe(false);
		expect(isPlainClick(click({ ctrlKey: true }))).toBe(false);
		expect(isPlainClick(click({ altKey: true }))).toBe(false);
		expect(isPlainClick(click({ shiftKey: true }))).toBe(false);
	});
});
