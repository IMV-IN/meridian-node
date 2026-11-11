# Releasing meridian-node

1. Bump `version` in `pyproject.toml` and add the entry to `CHANGELOG.md`; merge to `main` once CI is green.
2. Tag the merge commit and push the tag:
   ```bash
   git tag v0.1.1 && git push origin v0.1.1
   ```
   `.github/workflows/release.yml` fails early if the tag does not match the `pyproject.toml` version. It can also
   be run by hand (Actions, Release, Run workflow) with a `tag` input; a `v*` input is checked the same way, a
   free-form one such as `dev` is not.
3. The workflow builds `linux/amd64` and `linux/arm64` and pushes `<version>` and `latest` to:
   - `ghcr.io/<owner>/meridian-node` (always, using the built-in `GITHUB_TOKEN`);
   - `<DOCKERHUB_USERNAME>/meridian-node` (only when the Docker Hub secrets are set).

   It also builds the wheel and sdist (`python -m build`) and uploads them as the `meridian-node-dist` workflow
   artifact. It does not create a GitHub Release and does not publish to PyPI.

## Secrets (repository settings, Actions)

- `DOCKERHUB_USERNAME`: the Docker Hub namespace, `ys1nghc`.
- `DOCKERHUB_TOKEN`: a Docker Hub access token with Read & Write scope.

Without them only GHCR is published. Resulting images: `ys1nghc/meridian-node` and, with `<owner>` lowercased,
`ghcr.io/<owner>/meridian-node`.
