import { ConfirmDeleteDialog } from '@/components/confirm-delete-dialog';
import { Button, ButtonLoading } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import message from '@/components/ui/message';
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet';
import { Switch } from '@/components/ui/switch';
import { Textarea } from '@/components/ui/textarea';
import {
  IAgentWeComTestMessageResult,
  useDeleteAgentWeCom,
  useFetchAgentWeCom,
  useSaveAgentWeCom,
  useTestAgentWeComConnection,
  useTestAgentWeComMessage,
} from '@/hooks/use-agent-request';
import { IModalProps } from '@/interfaces/common';
import { cn } from '@/lib/utils';
import { useCallback, useEffect, useMemo, useState } from 'react';

type WeComSheetProps = IModalProps<any> & {
  agentId: string;
};

function formatTimestamp(timestamp?: number | null) {
  return timestamp ? new Date(timestamp * 1000).toLocaleString() : '-';
}

export function WeComSheet({ agentId, hideModal }: WeComSheetProps) {
  const { data, loading } = useFetchAgentWeCom(agentId);
  const { loading: saving, saveAgentWeCom } = useSaveAgentWeCom(agentId);
  const { loading: deleting, deleteAgentWeCom } = useDeleteAgentWeCom(agentId);
  const { loading: testingConnection, testAgentWeComConnection } =
    useTestAgentWeComConnection(agentId);
  const { loading: testingMessage, testAgentWeComMessage } =
    useTestAgentWeComMessage(agentId);

  const [enabled, setEnabled] = useState(false);
  const [botId, setBotId] = useState('');
  const [secret, setSecret] = useState('');
  const [secretTouched, setSecretTouched] = useState(false);
  const [testContent, setTestContent] = useState('');
  const [testMediaFixture, setTestMediaFixture] = useState('');
  const [testResult, setTestResult] = useState<IAgentWeComTestMessageResult>();
  const [connectionResult, setConnectionResult] = useState('');

  useEffect(() => {
    setEnabled(Boolean(data?.enabled));
    setBotId(data?.bot_id ?? '');
    setSecret('');
    setSecretTouched(false);
    setConnectionResult('');
    setTestResult(undefined);
    setTestMediaFixture('');
  }, [data]);

  const canSave =
    botId.trim().length > 0 &&
    (Boolean(data?.has_secret) || secret.trim().length > 0);
  const canTest = Boolean(data?.bot_id && data?.has_secret);
  const lastConnectedAt = useMemo(
    () => formatTimestamp(data?.last_connected_at),
    [data?.last_connected_at],
  );

  const handleSave = useCallback(async () => {
    const payload: { bot_id: string; secret?: string; enabled: boolean } = {
      bot_id: botId.trim(),
      enabled,
    };

    if (secretTouched && secret.trim()) {
      payload.secret = secret.trim();
    }

    await saveAgentWeCom(payload);
  }, [botId, enabled, saveAgentWeCom, secret, secretTouched]);

  const handleDelete = useCallback(async () => {
    await deleteAgentWeCom();
    setBotId('');
    setSecret('');
    setSecretTouched(false);
    setEnabled(false);
    setConnectionResult('');
    setTestResult(undefined);
  }, [deleteAgentWeCom]);

  const handleTestConnection = useCallback(async () => {
    const result = await testAgentWeComConnection();
    setConnectionResult(result?.message ?? '');
  }, [testAgentWeComConnection]);

  const handleTestMessage = useCallback(async () => {
    let mediaFixture:
      | Parameters<typeof testAgentWeComMessage>[0]['media']
      | undefined;
    if (testMediaFixture.trim()) {
      try {
        mediaFixture = JSON.parse(testMediaFixture);
      } catch {
        message.error('媒体 Fixture JSON 无效');
        return;
      }
    }
    const result = await testAgentWeComMessage({
      userid: 'debug-user',
      chatid: 'debug-chat',
      chattype: 'single',
      content: testContent,
      media: mediaFixture,
    });
    setTestResult(result);
  }, [testAgentWeComMessage, testContent, testMediaFixture]);

  return (
    <Sheet open onOpenChange={hideModal} modal={false}>
      <SheetContent
        className={cn('top-20 p-0 flex max-w-[720px] flex-col overflow-hidden')}
        onInteractOutside={(event) => event.preventDefault()}
      >
        <SheetHeader className="border-b border-border px-5 py-4">
          <SheetTitle>企业微信</SheetTitle>
        </SheetHeader>

        <div className="flex-1 space-y-5 overflow-auto px-5 py-4 text-sm">
          <section className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <Label>BotID</Label>
              <Input
                value={botId}
                onChange={(e) => setBotId(e.target.value)}
                placeholder="aib..."
                disabled={loading}
              />
            </div>
            <div className="space-y-2">
              <Label>Secret</Label>
              <Input
                value={secret}
                onChange={(e) => {
                  setSecret(e.target.value);
                  setSecretTouched(true);
                }}
                placeholder={
                  data?.has_secret ? '已保存，留空则不修改' : '请输入 Secret'
                }
                type="password"
                disabled={loading}
              />
            </div>
          </section>

          <section className="flex items-center justify-between rounded-md border border-border p-3">
            <div className="min-w-0 space-y-1">
              <div className="font-medium">启用企业微信渠道</div>
              <div className="break-words text-xs text-text-secondary">
                {data?.status || 'unbound'} · {lastConnectedAt}
                {data?.last_error ? ` · ${data.last_error}` : ''}
              </div>
            </div>
            <Switch
              checked={enabled}
              disabled={loading}
              onCheckedChange={setEnabled}
            />
          </section>

          <section className="grid gap-3 md:grid-cols-3">
            <ButtonLoading
              loading={saving}
              onClick={handleSave}
              variant="secondary"
              disabled={!canSave}
            >
              保存配置
            </ButtonLoading>
            <ButtonLoading
              loading={testingConnection}
              onClick={handleTestConnection}
              variant="secondary"
              disabled={!canTest}
            >
              测试连接
            </ButtonLoading>
            <ConfirmDeleteDialog
              title="解绑企业微信"
              content={{
                title: '确认解绑该智能体的企业微信机器人？',
              }}
              onOk={handleDelete}
              okButtonText="解绑"
              hidden={!data?.bot_id}
            >
              <Button
                variant="secondary"
                disabled={deleting || !data?.bot_id}
                loading={deleting}
              >
                解绑
              </Button>
            </ConfirmDeleteDialog>
          </section>

          {connectionResult && (
            <div className="rounded-md bg-bg-card p-3 whitespace-pre-wrap">
              {connectionResult}
            </div>
          )}

          <section className="space-y-3 rounded-md border border-border p-3">
            <Label>模拟消息</Label>
            <Textarea
              value={testContent}
              onChange={(e) => setTestContent(e.target.value)}
              placeholder="输入一条企业微信调试消息"
              rows={3}
              disabled={!canTest}
            />
            <Textarea
              value={testMediaFixture}
              onChange={(e) => setTestMediaFixture(e.target.value)}
              placeholder='{"type":"image","filename":"a.png","content_type":"image/png","data_base64":"..."}'
              rows={3}
              disabled={!canTest}
            />
            <ButtonLoading
              loading={testingMessage}
              onClick={handleTestMessage}
              variant="secondary"
              disabled={
                !canTest || (!testContent.trim() && !testMediaFixture.trim())
              }
            >
              发送模拟消息
            </ButtonLoading>
            {testResult && (
              <div className="space-y-2 rounded-md bg-bg-card p-3">
                <div className="whitespace-pre-wrap">
                  {testResult.reply || '-'}
                </div>
                <div className="grid gap-1 text-xs text-text-secondary md:grid-cols-2">
                  <div>Session: {testResult.session_id || '-'}</div>
                  <div>Frames: {testResult.frame_count ?? 0}</div>
                  <div>Finish: {testResult.finish ? 'true' : 'false'}</div>
                  <div>Stream: {testResult.stream_id || '-'}</div>
                </div>
                {testResult.image_urls && testResult.image_urls.length > 0 && (
                  <div className="space-y-1 text-xs text-text-secondary">
                    {testResult.image_urls.map((url) => (
                      <div key={url} className="break-all">
                        {url}
                      </div>
                    ))}
                  </div>
                )}
                {Boolean(
                  testResult.stored_references?.length ||
                  testResult.public_urls?.length ||
                  testResult.uploaded_media_ids?.length ||
                  testResult.rejected_media_reason ||
                  testResult.media_failures?.length,
                ) && (
                  <div className="space-y-1 text-xs text-text-secondary">
                    {testResult.rejected_media_reason && (
                      <div>Rejected: {testResult.rejected_media_reason}</div>
                    )}
                    {testResult.stored_references?.map((value) => (
                      <div key={value} className="break-all">
                        Stored: {value}
                      </div>
                    ))}
                    {testResult.public_urls?.map((value) => (
                      <div key={value} className="break-all">
                        Public: {value}
                      </div>
                    ))}
                    {testResult.uploaded_media_ids?.map((value) => (
                      <div key={value} className="break-all">
                        MediaID: {value}
                      </div>
                    ))}
                    {testResult.media_failures?.map((value) => (
                      <div key={value}>Media: {value}</div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </section>
        </div>
      </SheetContent>
    </Sheet>
  );
}
