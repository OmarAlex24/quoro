import { describe, expect, it } from 'vitest';
import { secretParts } from './secret-words';

const JOIN =
  'quoro join https://10.0.0.3:8443 --token njt_hd-R9yudcjc2JjGtbCrzw4gEKrj53yKm-6Xd8ANTSy0 --fingerprint 4f1c9a2e7b3d5f60a8c2e4b6d8f0a1c3e5b7d9f1a3c5e7b9d1f3a5c7e9b1d3f5 --role witness';

const joined = (value: string) =>
  secretParts(value)
    .map((part) => part.text)
    .join('');

describe('secretParts', () => {
  it('cuts a command at its spaces and keeps each word whole', () => {
    const words = secretParts(JOIN).filter((part) => !part.space).map((part) => part.text);
    expect(words).toEqual([
      'quoro',
      'join',
      'https://10.0.0.3:8443',
      '--token',
      'njt_hd-R9yudcjc2JjGtbCrzw4gEKrj53yKm-6Xd8ANTSy0',
      '--fingerprint',
      '4f1c9a2e7b3d5f60a8c2e4b6d8f0a1c3e5b7d9f1a3c5e7b9d1f3a5c7e9b1d3f5',
      '--role',
      'witness',
    ]);
  });

  it('leaves a value without spaces as one word', () => {
    expect(secretParts('qrk1-9Q9G-F0DN-WTCJ')).toEqual([{ text: 'qrk1-9Q9G-F0DN-WTCJ', space: false }]);
  });

  it('gives back the value exactly, runs of spaces and edges included', () => {
    for (const value of [JOIN, 'a  b', ' lead', 'trail ', '   ', 'one', '']) {
      expect(joined(value)).toBe(value);
    }
    expect(secretParts('a  b')).toEqual([
      { text: 'a', space: false },
      { text: '  ', space: true },
      { text: 'b', space: false },
    ]);
  });
});
