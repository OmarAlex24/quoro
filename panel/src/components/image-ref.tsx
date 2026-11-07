import { builtImageBuildId } from '../lib/build-status';

/**
 * An image as a row names it. A reference given by the operator is a code
 * value; an image Quoro built reads "built image" with only the build id in
 * the code face, since the words are a label.
 */
export function ImageRef({ image }: { image: string }) {
  const buildId = builtImageBuildId(image);
  if (buildId === null) return <span className="type-code">{image}</span>;
  return (
    <>
      built image
      {buildId === '' ? null : (
        <>
          {' '}
          <span className="type-code">{buildId}</span>
        </>
      )}
    </>
  );
}
