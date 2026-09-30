type ShortcutEvent = Pick<KeyboardEvent, 'key' | 'metaKey' | 'ctrlKey' | 'altKey' | 'shiftKey'>;

/** Reports whether a keydown opens search: ⌘K or Ctrl+K. */
export function isSearchShortcut(event: ShortcutEvent): boolean {
	return (
		event.key.toLowerCase() === 'k' &&
		(event.metaKey || event.ctrlKey) &&
		!event.altKey &&
		!event.shiftKey
	);
}

/**
 * Moves a selection by one through count items, wrapping at either end. From no
 * selection (-1), down selects the first item and up the last. Returns -1 when
 * there's nothing to select.
 */
export function moveSelection(current: number, delta: 1 | -1, count: number): number {
	if (count === 0) return -1;
	if (current < 0) return delta === 1 ? 0 : count - 1;
	return (current + delta + count) % count;
}

type ClickEvent = Pick<MouseEvent, 'button' | 'metaKey' | 'ctrlKey' | 'altKey' | 'shiftKey'>;

/**
 * Reports whether a click is a plain left click. Leave the others to the browser,
 * so a modified click can open the link in a new tab.
 */
export function isPlainClick(event: ClickEvent): boolean {
	return event.button === 0 && !event.metaKey && !event.ctrlKey && !event.altKey && !event.shiftKey;
}

/** Returns the shortcut's label for a platform string such as navigator.platform. */
export function shortcutLabel(platform: string): string {
	return /mac|iphone|ipad/i.test(platform) ? '⌘K' : 'Ctrl K';
}
