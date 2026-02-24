# Publish here-i-strand (HIS) to PyPI

## 1. Prepare the project

- **PyPI name**: Check if `here-i-strand` is available at [pypi.org/project/here-i-strand](https://pypi.org/project/here-i-strand). If taken, change `name` in `pyproject.toml` (e.g. `your-company-here-i-strand`).
- **Authors**: Update authors in `pyproject.toml` if needed.
- **README**: Ensure `README.md` exists at the project root (it is shown on the package page on PyPI).
- **License**: The project uses MIT; ensure `LICENSE` exists.

## 2. PyPI account and token

1. Register at [pypi.org](https://pypi.org/account/register/) (and optionally [test.pypi.org](https://test.pypi.org/account/register/) for testing).
2. Create an **API token**:
   - PyPI → Account settings → API tokens → Add API token
   - Descriptive name, scope “Entire account” or “Project: here-i-strand”
   - Copy the token (it is shown only once).

## 3. Build the package

From the repo root:

```bash
# With uv (recommended)
uv build

# Or with pip + build
pip install build
python -m build
```

This produces `dist/here_i_strand-0.1.0-py3-none-any.whl` and `dist/here-i-strand-0.1.0.tar.gz`.

## 4. Upload to PyPI

**Option A – With uv (recommended):**

```bash
uv publish
# When prompted: username __token__, password = your API token
```

**Option B – With twine:**

```bash
pip install twine
twine upload dist/*
# Username: __token__
# Password: <your API token>
```

To try Test PyPI first:

```bash
uv publish --index-url https://test.pypi.org/legacy/
# or
twine upload --repository testpypi dist/*
```

## 5. After publishing

- Check the package page at [pypi.org/project/here-i-strand](https://pypi.org/project/here-i-strand).
- For new versions: bump `version` in `pyproject.toml`, run `uv build` again, then `uv publish` (or `twine upload dist/*`).

## Quick summary

| Step | Command / action |
|------|-------------------|
| 1 | Set `name`, `authors`, README in `pyproject.toml` |
| 2 | Create account and API token on pypi.org |
| 3 | `uv build` |
| 4 | `uv publish` (username: `__token__`, password: API token) |
