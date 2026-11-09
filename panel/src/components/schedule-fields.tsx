import type { ScheduleErrors, ScheduleForm } from '../lib/backup-forms';
import { Field } from './ui/field';
import { Input } from './ui/input';

/**
 * The two numbers of a backup schedule, `interval_h` and `keep`, with the
 * words the app's Storage tab uses. The app's schedule and the control-plane
 * schedule of the backup repository both use these fields.
 */
export function ScheduleFields({
  idPrefix,
  value,
  errors,
  disabled = false,
  onChange,
}: {
  idPrefix: string;
  value: ScheduleForm;
  errors: ScheduleErrors;
  disabled?: boolean;
  onChange: (value: ScheduleForm) => void;
}) {
  return (
    <div className="flex flex-col gap-3 sm:flex-row sm:items-start">
      <Field
        label="Every (hours)"
        htmlFor={`${idPrefix}-interval`}
        hint="Counted from the last succeeded backup."
        className="sm:flex-1"
      >
        <Input
          id={`${idPrefix}-interval`}
          type="number"
          min={1}
          max={168}
          step={1}
          inputMode="numeric"
          value={value.interval}
          disabled={disabled}
          onChange={(event) => onChange({ ...value, interval: event.target.value })}
          aria-invalid={errors.interval !== undefined}
        />
        {errors.interval ? <p className="text-[12.5px] text-fault">{errors.interval}</p> : null}
      </Field>
      <Field
        label="Keep"
        htmlFor={`${idPrefix}-keep`}
        hint="Succeeded backups kept; older ones are pruned."
        className="sm:flex-1"
      >
        <Input
          id={`${idPrefix}-keep`}
          type="number"
          min={1}
          max={100}
          step={1}
          inputMode="numeric"
          value={value.keep}
          disabled={disabled}
          onChange={(event) => onChange({ ...value, keep: event.target.value })}
          aria-invalid={errors.keep !== undefined}
        />
        {errors.keep ? <p className="text-[12.5px] text-fault">{errors.keep}</p> : null}
      </Field>
    </div>
  );
}
