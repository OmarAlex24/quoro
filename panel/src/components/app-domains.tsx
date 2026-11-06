import { useMutation } from '@tanstack/react-query';
import { useState } from 'react';
import { createAppDomain, deleteDomain } from '../api/generated/domains/domains';
import type {
  CreateDomainRequestTlsMode,
  Domain,
} from '../api/generated/model';
import {
  certTone,
  certWords,
  servingTone,
  servingWords,
  tlsWords,
} from '../lib/app-status';
import { problemView, type ProblemView } from '../lib/problem';
import { Button } from './ui/button';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';
import { ToneLamp } from './tone-lamp';

export function AppDomains({
  appId,
  domains,
  canWrite,
  onChanged,
}: {
  appId: string;
  domains: readonly Domain[];
  canWrite: boolean;
  onChanged: () => void;
}) {
  const [hostname, setHostname] = useState('');
  const [tlsMode, setTlsMode] = useState<CreateDomainRequestTlsMode>('tunnel');
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const add = useMutation({
    mutationFn: () =>
      createAppDomain(
        appId,
        { hostname: hostname.trim(), tls_mode: tlsMode },
        { headers: { 'Idempotency-Key': crypto.randomUUID() } },
      ),
    onSuccess: () => {
      setProblem(null);
      setHostname('');
      onChanged();
    },
    onError: (error) => setProblem(problemView(error, 'add domains')),
  });

  const remove = useMutation({
    mutationFn: (domainId: string) => deleteDomain(domainId),
    onSuccess: () => {
      setProblem(null);
      onChanged();
    },
    onError: (error) => setProblem(problemView(error, 'remove domains')),
  });

  return (
    <section>
      <h2 className="type-display-light text-[15px] leading-none text-ink">Domains</h2>

      {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}

      <ul className="mt-3 flex flex-col divide-y divide-rail border-y border-rail">
        {domains.length === 0 ? (
          <li className="py-3 text-[13px] text-ink-muted">
            No domains yet. Add a hostname to route traffic to this app.
          </li>
        ) : (
          domains.map((domain) => (
            <li
              key={domain.id}
              className="flex flex-wrap items-center gap-x-3 gap-y-2 py-2.5"
            >
              <span className="type-data min-w-0 flex-1 truncate text-[14px] text-ink">
                {domain.hostname}
              </span>
              <span className="text-[13px] text-ink-muted">{tlsWords[domain.tls_mode]}</span>
              <span className="inline-flex items-center gap-1.5 text-[13px] text-ink-muted">
                <ToneLamp
                  tone={certTone(domain.cert_status)}
                  size={13}
                  label={certWords[domain.cert_status]}
                />
                {certWords[domain.cert_status]}
              </span>
              <span className="inline-flex items-center gap-1.5 text-[13px] text-ink-muted">
                <ToneLamp
                  tone={servingTone(domain.serving_status)}
                  size={13}
                  label={servingWords[domain.serving_status]}
                />
                {servingWords[domain.serving_status]}
              </span>
              {canWrite ? (
                <Button
                  variant="quiet"
                  className="px-2.5"
                  disabled={remove.isPending}
                  onClick={() => {
                    setProblem(null);
                    remove.mutate(domain.id);
                  }}
                >
                  Remove
                </Button>
              ) : null}
              {domain.serving_status === 'unserved' && domain.last_error ? (
                <p className="basis-full text-[13px] leading-snug text-lamp-fault">
                  {domain.last_error}
                </p>
              ) : null}
            </li>
          ))
        )}
      </ul>

      {canWrite ? (
        <form
          className="mt-3 flex flex-col gap-2 sm:flex-row sm:items-center"
          onSubmit={(event) => {
            event.preventDefault();
            if (hostname.trim() === '') {
              setProblem({ message: 'Enter a hostname to add.' });
              return;
            }
            setProblem(null);
            add.mutate();
          }}
        >
          <Input
            aria-label="New domain hostname"
            value={hostname}
            onChange={(event) => setHostname(event.target.value)}
            placeholder="blog.example.com"
            spellCheck={false}
            className="sm:flex-1"
          />
          <select
            aria-label="TLS mode"
            className="field sm:w-[190px]"
            value={tlsMode}
            onChange={(event) =>
              setTlsMode(event.target.value as CreateDomainRequestTlsMode)
            }
          >
            <option value="tunnel">Cloudflare Tunnel</option>
            <option value="acme">ACME certificate</option>
          </select>
          <Button variant="secondary" type="submit" disabled={add.isPending}>
            {add.isPending ? 'Adding…' : 'Add domain'}
          </Button>
        </form>
      ) : null}
    </section>
  );
}
