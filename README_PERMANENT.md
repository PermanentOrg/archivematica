# Permanent-Specific Documentation

## Branching Strategy

The `main` branch in this repo is intended to match the latest release of
Archivematica, plus any alterations we've made for our fork specifically.
Such alterations could include changes that we want to commit upstream but
haven't made it into a release yet, as well as changes that are irrelevant
to upstream (such as the Github Action that builds Docker images for
Archivematica and uploads them to a Permanent-controlled ECR repository).
The `qa` branch is intended to match the upstream `qa/1.x` branch. Feature
branches should branch from `main` and undergo code review within the
Permanent fork. If a feature branch is meant to be an upstream contribution,
it should be rebased from `qa` once it's been merged into `main`, then a PR
should be opened against upstream's `qa/1.x` branch.

## Deployment

Deploys are triggered through our infrastructure repo. The deploy job there
triggers the build job here, then instructs terraform to update our EKS cluster
to start using the new images built by that job.
