import { describe, expect, mock, test } from 'bun:test';

const updateCalls: Array<Record<string, unknown>> = [];

const sdkClient = {
  session: {
    update: mock((params: Record<string, unknown>) => {
      updateCalls.push(params);
      return Promise.resolve({
        data: {
          id: params.sessionID,
          title: 'Restored session',
          time: { created: 1 },
        },
      });
    }),
  },
};

mock.module('@opencode-ai/sdk/v2', () => ({
  OpencodeClient: class {},
  createOpencodeClient: mock(() => sdkClient),
}));

describe('opencodeClient.updateSession', () => {
  test('passes archived null through to the SDK update payload', async () => {
    const { opencodeClient } = await import('./client');

    await opencodeClient.updateSession('ses_1', { time: { archived: null } }, '/repo/app');

    expect(updateCalls).toEqual([
      {
        sessionID: 'ses_1',
        directory: '/repo/app',
        time: { archived: null },
      },
    ]);
  });
});
