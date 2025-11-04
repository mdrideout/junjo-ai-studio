/**
 * MSW (Mock Service Worker) server configuration for auth integration tests.
 *
 * This file sets up request handlers to mock backend API responses during tests.
 * Individual tests can override these handlers using server.use() for specific scenarios.
 */

import { setupServer } from 'msw/node'
import { http, HttpResponse } from 'msw'

// Base URL for API requests (matches development environment)
const API_BASE = 'http://localhost:1323'

/**
 * Default request handlers for common API endpoints.
 * Tests can override these using server.use() for custom scenarios.
 */
export const handlers = [
  // Mock /api_keys endpoint - returns empty array by default (no API keys)
  http.get(`${API_BASE}/api_keys`, () => {
    return HttpResponse.json([])
  }),

  // Mock /users/create-first-user endpoint - successful user creation
  http.post(`${API_BASE}/users/create-first-user`, () => {
    return HttpResponse.json(
      { message: 'First user created successfully' },
      { status: 200 }
    )
  }),

  // Mock /sign-in endpoint - successful sign-in
  http.post(`${API_BASE}/sign-in`, () => {
    return HttpResponse.json({ message: 'signed in' }, { status: 200 })
  }),

  // Mock /auth-test endpoint - user is authenticated
  http.get(`${API_BASE}/auth-test`, () => {
    return HttpResponse.json({ user_email: 'test@example.com' }, { status: 200 })
  }),

  // Mock /users/db-has-users endpoint - database has users after creation
  http.get(`${API_BASE}/users/db-has-users`, () => {
    return HttpResponse.json({ users_exist: true }, { status: 200 })
  }),
]

/**
 * MSW server instance configured with default handlers.
 * Import this in test-setup.ts to start the server before tests.
 */
export const server = setupServer(...handlers)
