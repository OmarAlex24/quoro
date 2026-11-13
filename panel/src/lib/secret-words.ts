// SPDX-License-Identifier: AGPL-3.0-only

/** One piece of a one-time value: a word, or the run of spaces between two words. */
export interface SecretPart {
  text: string;
  space: boolean;
}

/**
 * A one-time value (a key, a token, a join command) cut at its spaces for
 * display, so each word can be kept whole on a line. Joined back in order the
 * parts are the value exactly, spaces included.
 */
export const secretParts = (value: string): SecretPart[] =>
  value
    .split(/( +)/)
    .filter((text) => text !== '')
    .map((text) => ({ text, space: text.startsWith(' ') }));
