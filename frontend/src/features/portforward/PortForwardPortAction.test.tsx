import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })

const mocks = vi.hoisted(() => ({ stopPortForward: vi.fn(), browserOpenURL: vi.fn() }))

vi.mock('@/lib/api', () => ({ api: { stopPortForward: mocks.stopPortForward } }))
vi.mock('@/lib/wails/wailsjs/runtime/runtime', () => ({ BrowserOpenURL: mocks.browserOpenURL }))
vi.mock('./PortForwardButton', () => ({
  PortForwardDialog: ({ open, initialPort }: { open: boolean; initialPort?: number }) =>
    open ? <div role="dialog">forward {initialPort}</div> : null,
}))

import type { PortForwardInfo } from '@/lib/api'
import { usePortForwards } from '@/store/portForwards'
import { PortForwardPortAction } from './PortForwardPortAction'

const target = { kind: 'Service' as const, namespace: 'default', name: 'web' }
const forward = (over: Partial<PortForwardInfo>) =>
  ({
    id: 'pf-1',
    context: 'ctx',
    namespace: 'default',
    kind: 'Service',
    name: 'web',
    port: 80,
    localPort: 0,
    protocol: 'http',
    enabled: true,
    status: 'active',
    boundPort: 30800,
    podName: 'web-0',
    targetPort: 8080,
    error: '',
    ...over,
  }) as PortForwardInfo

describe('PortForwardPortAction', () => {
  let container: HTMLDivElement
  let root: Root

  beforeEach(() => {
    vi.clearAllMocks()
    mocks.stopPortForward.mockResolvedValue(undefined)
    usePortForwards.setState({ list: [] })
    container = document.createElement('div')
    document.body.appendChild(container)
    root = createRoot(container)
  })

  afterEach(() => {
    act(() => root.unmount())
    container.remove()
  })

  const render = (port = 80, protocol?: string) =>
    act(async () =>
      root.render(
        <QueryClientProvider client={new QueryClient()}>
          <PortForwardPortAction contextName="ctx" target={target} port={port} protocol={protocol} />
        </QueryClientProvider>,
      ),
    )

  it('offers Forward for a port without a running forward and opens the dialog on it', async () => {
    usePortForwards.setState({ list: [forward({ status: 'stopped', enabled: false, boundPort: 0 })] })
    await render()
    const button = container.querySelector<HTMLButtonElement>('button[aria-label="Forward port 80"]')!
    await act(async () => button.click())
    expect(container.querySelector('[role="dialog"]')?.textContent).toBe('forward 80')
  })

  it('links to the running forward and stops it', async () => {
    usePortForwards.setState({ list: [forward({})] })
    await render()
    const link = [...container.querySelectorAll('button')].find((b) => b.textContent?.includes('localhost:30800'))!
    await act(async () => link.click())
    expect(mocks.browserOpenURL).toHaveBeenCalledWith('http://localhost:30800')
    await act(async () => container.querySelector<HTMLButtonElement>('button[aria-label="Stop forwarding port 80"]')!.click())
    expect(mocks.stopPortForward).toHaveBeenCalledWith('pf-1')
  })

  it('shows a reconnecting forward without a link', async () => {
    usePortForwards.setState({ list: [forward({ status: 'reconnecting', boundPort: 0, error: 'pod web-0 is gone' })] })
    await render()
    expect(container.textContent).toContain('Reconnecting…')
    expect(container.querySelector('button[aria-label="Stop forwarding port 80"]')).not.toBeNull()
  })

  it('does not offer UDP ports', async () => {
    await render(53, 'UDP')
    expect(container.querySelector('button')).toBeNull()
  })
})
