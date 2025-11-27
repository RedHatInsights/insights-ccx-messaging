# AGENTS.md

## Project Overview
insights-ccx-messaging is a Python-based framework for the Red Hat Insights ecosystem, built on top of insights-core-messaging. It provides a plugin-based architecture for consuming, processing, and publishing messages in different technologies like Kafka, S3...

**Tech Stack**: Python 3.11+, Kafka (confluent-kafka), boto3/S3, Prometheus, Sentry/Glitchtip

## Repository Structure
```text
/ccx_messaging/          - Main source code (all importable components)
  /consumers/            - Kafka message consumers (various message types)
  /downloaders/          - Archive downloaders (HTTP, S3)
  /engines/              - Processing engines (currently minimal)
  /publishers/           - Kafka message publishers (various output formats)
  /watchers/             - Monitoring components (stats, cluster tracking)
  /utils/                - Utility functions and helpers
  command_line.py        - CLI entry point (ccx-messaging command)
  schemas.py             - JSON schemas for message validation
  ingress.py             - Message parsing and validation utilities
  error.py               - Custom exception classes

/test/                   - Test files (mirror src structure)
  /consumers/            - Consumer tests
  /downloaders/          - Downloader tests
  /publishers/           - Publisher tests
  /watchers/             - Watcher tests
  *_test.py              - Unit tests (suffix: _test.py)

/deploy/                 - Deployment configurations
/docs/                   - Documentation
config.yaml              - Example of configuration
Dockerfile               - Container image generation script
pyproject.toml           - Python project definition file

/.github/workflows       - GitHub Actions CI configuration
```

## Development Workflow

### Setup
- **Python versions**: 3.11, 3.12 supported
- **Install dependencies**: `pip install -r requirements.txt`
- **Install dev dependencies**: `pip install -e ".[dev,test]"`

### Running Tests
- `make tests` - Run all tests
- `make coverage` - Run tests with coverage display
- `make coverage-report` - Generate HTML coverage report (in htmlcov/)
- `make documentation` - Generate literate documentation of the source code
- `pytest -v -p no:cacheprovider` - Run pytest directly

### Code Quality
- `make lint` - Check code style with ruff
- `make pyformat` - Auto-format all Python code with ruff
- `make shellcheck` - Lint shell scripts
- `pre-commit run --all-files` - Run all pre-commit hooks

### Documentation
- `make documentation` - Generate pydoc documentation
- Docs available at: <https://redhatinsights.github.io/insights-ccx-messaging/>
- Docs are generated using a Jekyll site placed in the `/docs` directory.

## Key Architectural Patterns

### Plugin-Based Architecture

The service is configured via YAML and uses a plugin system from insights-core-messaging:

- **Consumer**: Reads messages from a source (Kafka for example)
- **Downloader**: Fetches archives (HTTP or S3 for example)
- **Engine**: Processes data (optional, a default Engine is provided)
- **Publisher**: Sends results to a different service (Kafka or S3 for example)
- **Watchers**: monitor service events, for example for exposing metrics. They use the observer design pattern.

All components are specified in `config.yaml` with fully qualified class names.

### Message Flow
1. Consumer receives a message with a relevant archive to be processed
2. Downloader fetches archive based on message metadata
3. Engine processes archive
4. Publisher sends results to a different service
5. Watchers monitor service events. Example events: downloaded archive, received message, processed message, sent message...

#### Example
1. Consumer receives a message from Kafka topic "A" with "url" attribute.
2. Downloader fetches the archive from the URL specified in the received message.
3. Engine processes the archive using different Insights rules.
4. Publisher encodes the result as a JSON message and sends it to a Kafka topic "B".
5. Watchers monitor service events in parallel, for example, exposing Prometheus metrics.

### Configuration
- **Primary config**: `config.yaml` (YAML format)
- **Structure**: Plugin-based with `name` (class path) and `kwargs` (parameters)
- **Logging**: Python logging configuration
- **Service discovery**: app-common-python can be used for cloud platform integration

## Code Conventions

### Python Style
- **Line length**: 100 characters (configured in pyproject.toml)
- **Linter**: ruff with select rules: E, F, W, UP, C, D
- **Formatter**: ruff format
- **Docstrings**: Google style (REQUIRED for all public methods)
- **Type hints**: Not strictly enforced but encouraged

### Naming Patterns
- Test files: `*_test.py` (e.g., `kafka_consumer_test.py`)
- Consumer classes: `*Consumer` (e.g., `KafkaConsumer`, `SyncedArchiveConsumer`)
- Publisher classes: `*Publisher` (e.g., `RuleProcessingPublisher`)
- Downloader classes: `*Downloader` (e.g., `HTTPDownloader`, `S3Downloader`)
- Watcher classes: `*Watcher` (e.g., `StatsWatcher`, `ClusterIdWatcher`)

### Error Handling
- Custom exceptions in `error.py` (base: `CCXMessagingError`)
- Always log warnings/errors before raising exceptions
- Use structured logging with context (logger from `logging.getLogger(__name__)`)

### Message Validation
- All incoming messages MUST be validated against JSON schemas (see `schemas.py`)
- Use `ingress.py` functions for parsing: `parse_archive_sync_msg()`, `parse_rules_results_msg()`
- Invalid messages should be logged and rejected (optionally sent to DLQ)

## Important Notes

### Dependencies
- **insights-core-messaging**: Installed from git (not PyPI), version pinned in requirements.txt
- **boto3 version cap**: Limited to <1.40.60 due to s3fs incompatibility
- **Kafka**: Uses confluent-kafka (NOT kafka-python)

### Testing
- Tests use pytest with fixtures
- Test data available in `/test/` (e.g., `correct_data.tar`, `rapid-recommendations.tar.gz`)
- Use `freezegun` for time mocking when needed
- Coverage threshold: Aim to maintain/improve existing coverage

### Monitoring
- **Prometheus metrics**: Exposed on port 8000 (configurable via StatsWatcher)
- **Sentry**: Error tracking enabled (configure via environment/config)
- **CloudWatch**: Supported via watchtower integration

### Pre-commit Hooks
Repository uses pre-commit hooks:
- trailing-whitespace removal
- end-of-file-fixer
- YAML validation
- ruff linting and formatting
- pyupgrade for modern Python syntax

## Pull Request Guidelines

### Required Before PR
1. All tests must pass (`make unit_tests`)
2. Code must be formatted (`make pyformat`)
3. Linting must pass (`make lint`)
4. Coverage should not decrease
5. Google-style docstrings for all new public methods

### PR Requirements
- **Minimum 2 reviews** required from core team members
- **Descriptive commit messages** (explain why, not what)
- **Reference issues**: Include "closes #xyz" or "fixes #xyz"
- **Documentation**: Update docs for behavior changes
- **Base branch**: `main` (previously `master`)
- **WIP PRs**: Tag with `[WIP]` to prevent accidental merging

### Commit Message Format
Follow conventional style:
- No specific commit message convention.
- If it is related to one specific Jira task, use the Jira task ID. For example, "[CCXDEV-12345] Fixes the problem"

## Common Tasks

### Adding a New Consumer
1. Create class in `/ccx_messaging/consumers/my_consumer.py`
2. Inherit from appropriate base (or insights-core-messaging base)
3. Implement required methods with Google-style docstrings
4. Add tests in `/test/consumers/my_consumer_test.py`
5. Update `config.yaml` to reference new consumer
6. Update this file if architectural patterns change

### Adding a New Publisher
1. Create class in `/ccx_messaging/publishers/my_publisher.py`
2. Implement publish methods
3. Add schema validation if needed (update `schemas.py`)
4. Add tests in `/test/publishers/my_publisher_test.py`
5. Configure in `config.yaml`

### Modifying Message Schemas
1. Update schemas in `schemas.py`
2. Update parsing functions in `ingress.py` if needed
3. Add/update tests for schema validation
4. Document breaking changes in PR description

## Security Considerations
- **Never log sensitive data**: Account numbers, auth tokens, PII
- **Kafka credentials**: Use environment variables, not hardcoded values
- **S3 access**: Use IAM roles when possible, not access keys
- **Input validation**: Always validate incoming messages against schemas
- **Archive size limits**: Respect `max_archive_size` in downloader config

## Debugging Tips
- **Enable DEBUG logging**: Set `level: DEBUG` in config.yaml logging section
- **Local Kafka**: Use docker-compose or configure `bootstrap.servers` to localhost
- **Test with sample data**: Use archives in `/test/` directory
- **Prometheus metrics**: Check <http://localhost:8000/metrics> for service health
- **DLQ inspection**: Check dead_letter_queue_topic for failed messages

## External References
- [insights-core-messaging](https://github.com/RedHatInsights/insights-core-messaging) - Base framework
- [GitHub Pages Docs](https://redhatinsights.github.io/insights-ccx-messaging/)
- [confluent-kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Google Style Docstrings](https://www.sphinx-doc.org/en/master/usage/extensions/example_google.html#example-google)
