# 0001. Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-09-26

## Context

The platform spans three businesses and many components, and its decisions carry cost trade-offs that will be revisited. A design document explains the current state, but not how or why it changed over time.

## Decision

Architecture decisions are recorded as short, numbered ADRs in `docs/adr/`, using `0000-template.md`. An ADR is never edited after it's accepted, except to change its status. A change of mind is a new ADR that supersedes the old one.

## Consequences

- Reviewers can see the reasoning and the rejected options, not just the outcome.
- `docs/design.md` stays the readable overview and links here for the reasoning.
- Adding an ADR is part of any change that alters a decision below (see `CONTRIBUTING.md`).

## Alternatives considered

- **Decisions only in the design doc:** it loses history each time the doc is rewritten.
- **Wiki pages:** they drift from the code and aren't reviewed in pull requests.
