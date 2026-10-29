import { describe, expect, it } from 'vitest'
import type { PortForwardInfo } from '@/lib/api'
import { claimedLocalPorts, findForward, forwardURL, isRunning, suggestLocalPort } from './portForwards'

const pf = (over: Partial<PortForwardInfo>): PortForwardInfo =>
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

describe('port-forward helpers', () => {
  it('builds the browser link from the bound port and protocol', () => {
    expect(forwardURL(pf({}))).toBe('http://localhost:30800')
    expect(forwardURL(pf({ protocol: 'https', boundPort: 8443 }))).toBe('https://localhost:8443')
  })

  it('treats a retrying forward as running', () => {
    expect(isRunning(pf({ status: 'reconnecting' }))).toBe(true)
    expect(isRunning(pf({ status: 'idle' }))).toBe(false)
    expect(isRunning(pf({ status: 'error' }))).toBe(false)
  })

  it('matches a forward by context, kind, namespace, name and port', () => {
    const list = [pf({ id: 'a' }), pf({ id: 'b', kind: 'Pod' }), pf({ id: 'c', port: 443 })]
    const target = { kind: 'Service' as const, namespace: 'default', name: 'web' }
    expect(findForward(list, 'ctx', target, 80)?.id).toBe('a')
    expect(findForward(list, 'ctx', target, 443)?.id).toBe('c')
    expect(findForward(list, 'other', target, 80)).toBeUndefined()
  })

  it('suggests a local port no saved forward claims', () => {
    const used = claimedLocalPorts([pf({ localPort: 8080, boundPort: 0 }), pf({ boundPort: 16080 })])
    expect(suggestLocalPort(8080, used)).toBe(18080)
    expect(suggestLocalPort(80, used)).toBe(10080)
    expect(suggestLocalPort(3000, new Set())).toBe(3000)
  })
})
