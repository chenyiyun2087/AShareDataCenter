const API_BASE_URL = import.meta.env.VITE_API_BASE_URL as string | undefined;

type QueryParams = Record<string, string | number | boolean | undefined | null>;

const buildUrl = (path: string, params?: QueryParams) => {
  const url = API_BASE_URL
    ? new URL(path, API_BASE_URL)
    : new URL(path, window.location.origin);
  if (params) {
    Object.entries(params).forEach(([key, value]) => {
      if (value === undefined || value === null || value === '') {
        return;
      }
      url.searchParams.set(key, String(value));
    });
  }
  return url.toString();
};

const parseError = async (response: Response) => {
  const contentType = response.headers.get('content-type') || '';
  if (contentType.includes('application/json')) {
    const data = (await response.json()) as { error?: string };
    return data.error || response.statusText;
  }
  const text = await response.text();
  return text || response.statusText;
};

export const getJson = async <T>(path: string, params?: QueryParams): Promise<T> => {
  const response = await fetch(buildUrl(path, params));
  if (!response.ok) {
    throw new Error(await parseError(response));
  }
  return (await response.json()) as T;
};

export const postJson = async <T>(
  path: string,
  body: Record<string, unknown>,
): Promise<T> => {
  const response = await fetch(buildUrl(path), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    throw new Error(await parseError(response));
  }
  return (await response.json()) as T;
};

export const deleteJson = async <T>(path: string): Promise<T> => {
  const response = await fetch(buildUrl(path), {
    method: 'DELETE',
  });
  if (!response.ok) {
    throw new Error(await parseError(response));
  }
  return (await response.json()) as T;
};
