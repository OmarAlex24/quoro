/**
 * The one line the login screen says after a successful setup. Setup does not
 * start a session (the contract returns the user, not a cookie), so the panel
 * carries the news across the navigation in memory.
 */
let notice: string | undefined;

export const setSetupNotice = (value: string) => {
  notice = value;
};

export const takeSetupNotice = (): string | undefined => {
  const value = notice;
  notice = undefined;
  return value;
};
