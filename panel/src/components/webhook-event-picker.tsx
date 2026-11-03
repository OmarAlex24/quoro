import { EventType } from '../api/generated/model';
import { eventTypesBySubject } from '../lib/webhooks';

/**
 * Checkboxes for every event type, grouped by subject. Leaving all of them
 * unchecked subscribes the receiver to every event.
 */
export function WebhookEventPicker({
  id,
  value,
  onChange,
  disabled = false,
}: {
  id: string;
  value: readonly EventType[];
  onChange: (types: EventType[]) => void;
  disabled?: boolean;
}) {
  const selected = new Set(value);
  const toggle = (type: EventType) => {
    const next = new Set(selected);
    if (next.has(type)) next.delete(type);
    else next.add(type);
    // Keep contract order so the request and the summary read the same way.
    onChange(Object.values(EventType).filter((candidate) => next.has(candidate)));
  };

  return (
    <div className="flex flex-col gap-2">
      <fieldset id={id} disabled={disabled} className="grid gap-x-5 gap-y-2.5 sm:grid-cols-2">
        {eventTypesBySubject().map((group) => (
          <div key={group.subject} className="flex flex-col gap-1">
            <span className="type-body-medium text-[13px] capitalize text-ink-muted">
              {group.subject}
            </span>
            {group.types.map((type) => (
              <label
                key={type}
                className="inline-flex cursor-pointer items-center gap-1.5 text-[13px]"
              >
                <input
                  type="checkbox"
                  className="h-3.5 w-3.5 accent-cobalt"
                  checked={selected.has(type)}
                  onChange={() => toggle(type)}
                />
                <span className="type-data">{type}</span>
              </label>
            ))}
          </div>
        ))}
      </fieldset>
      <p className="text-[13px] leading-snug text-ink-muted">
        Leave all unchecked to send every event.
      </p>
    </div>
  );
}
