/**
 * Map a recipient email address to a mailbox provider and domain.
 * This is derived server-side — never trust the client for these.
 */

const PROVIDER_DOMAINS: Record<string, string> = {
  "gmail.com": "gmail",
  "googlemail.com": "gmail",
  "outlook.com": "outlook",
  "hotmail.com": "outlook",
  "live.com": "outlook",
  "msn.com": "outlook",
  "yahoo.com": "yahoo",
  "yahoo.co.uk": "yahoo",
  "aol.com": "yahoo",
  "icloud.com": "apple",
  "me.com": "apple",
  "mac.com": "apple",
  "zoho.com": "zoho",
  "proton.me": "proton",
  "protonmail.com": "proton",
  "mail.ru": "mailru",
  "yandex.ru": "yandex",
};

export function parseEmail(email: string): {
  local: string;
  domain: string;
} {
  const atIndex = email.lastIndexOf("@");
  if (atIndex < 1) {
    return { local: email, domain: "" };
  }
  return {
    local: email.slice(0, atIndex),
    domain: email.slice(atIndex + 1).toLowerCase(),
  };
}

export function detectMailboxProvider(email: string): string {
  const { domain } = parseEmail(email);
  return PROVIDER_DOMAINS[domain] ?? "other";
}

export function extractDomain(email: string): string {
  return parseEmail(email).domain;
}
