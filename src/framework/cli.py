"""
CLI interface for the data transformation framework
"""

import click
from pathlib import Path
from datetime import datetime
import sys

from framework.config import load_config
from framework.connection import create_executor
from framework.parser import SQLParser
from framework.model import load_model_registry
from framework.dependency import build_dependency_graph
from framework.state import create_state_manager
from framework.plan import create_plan_generator
from framework.executor import ModelExecutor
from framework.materialization import Materializer
from framework.testing import TestRunner
from framework.backfill import BackfillExecutor
from framework.watcher import create_file_watcher
from utils.logger import get_logger, setup_file_logging
from utils.errors import FrameworkError

logger = get_logger(__name__)


@click.group()
@click.option('--project-dir', default='.', help='Project directory')
@click.option('--log-level', default='INFO', help='Log level')
@click.pass_context
def main(ctx, project_dir, log_level):
    """Data Transformation Framework CLI"""
    ctx.ensure_object(dict)
    ctx.obj['project_dir'] = Path(project_dir)
    ctx.obj['log_level'] = log_level
    
    # Setup logging
    logger.logger.setLevel(log_level)


@main.command()
@click.argument('models', nargs=-1)
@click.option('--vars', help='Variables as key=value pairs, comma-separated')
@click.option('--dry-run', is_flag=True, help='Validate without executing')
@click.option('--target', help='Target environment')
@click.pass_context
def run(ctx, models, vars, dry_run, target):
    """Run one or more models"""
    try:
        project_dir = ctx.obj['project_dir']
        
        # Load configuration
        config = load_config(project_dir)
        if target:
            config.set_target(target)
        
        # Parse variables
        variables = {}
        if vars:
            for pair in vars.split(','):
                key, value = pair.split('=')
                variables[key.strip()] = value.strip()
        
        # Initialize components
        conn_config = config.get_connection_config()
        sf_executor = create_executor(conn_config)
        parser = SQLParser(config.get_models_dir())
        model_executor = ModelExecutor(sf_executor, parser, config=config, fail_fast=False)
        
        # Run models
        if not models:
            click.echo("No models specified. Use 'run-all' to run all models.")
            return
        
        click.echo(f"Running {len(models)} model(s)...")
        results = model_executor.execute_models(list(models), variables, dry_run)
        
        # Display results
        for result in results:
            status = result['status']
            model = result['model_name']
            if status in ['success', 'validated']:
                click.secho(f"✓ {model}: {status}", fg='green')
            else:
                click.secho(f"✗ {model}: {status} - {result.get('error', '')}", fg='red')
        
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        if ctx.obj['log_level'] == 'DEBUG':
            raise
        sys.exit(1)


@main.command()
@click.option('--vars', help='Variables as key=value pairs')
@click.option('--target', help='Target environment')
@click.pass_context
def run_all(ctx, vars, target):
    """Run all models"""
    try:
        project_dir = ctx.obj['project_dir']
        
        config = load_config(project_dir)
        if target:
            config.set_target(target)
        
        # Parse variables
        variables = {}
        if vars:
            for pair in vars.split(','):
                key, value = pair.split('=')
                variables[key.strip()] = value.strip()
        
        # Initialize
        conn_config = config.get_connection_config()
        sf_executor = create_executor(conn_config)
        parser = SQLParser(config.get_models_dir())
        
        # Parse all models
        parsed_models = parser.parse_directory()
        
        # Build dependency graph and get execution order
        dep_graph = build_dependency_graph(parsed_models)
        execution_order = dep_graph.topological_sort()
        
        click.echo(f"Executing {len(parsed_models)} models in {len(execution_order)} levels...")
        
        model_executor = ModelExecutor(sf_executor, parser, config=config, fail_fast=False)
        
        # Execute models level by level
        total_success = 0
        total_failed = 0
        
        for level_idx, level_models in enumerate(execution_order):
            click.echo(f"\nLevel {level_idx + 1}/{len(execution_order)}: {len(level_models)} models")
            results = model_executor.execute_models(level_models, variables)
            
            for result in results:
                if result['status'] == 'success':
                    total_success += 1
                    click.secho(f"  ✓ {result['model_name']}", fg='green')
                else:
                    total_failed += 1
                    click.secho(f"  ✗ {result['model_name']}: {result.get('error', '')}", fg='red')
        
        click.echo(f"\nCompleted: {total_success} succeeded, {total_failed} failed")
        
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        sys.exit(1)


@main.command()
@click.option('--models', help='Specific models to include in plan')
@click.option('--target', help='Target environment')
@click.pass_context
def plan(ctx, models, target):
    """Generate execution plan"""
    try:
        project_dir = ctx.obj['project_dir']
        
        config = load_config(project_dir)
        if target:
            config.set_target(target)
        
        # Initialize
        parser = SQLParser(config.get_models_dir())
        parsed_models = parser.parse_directory()
        dep_graph = build_dependency_graph(parsed_models)
        state_manager = create_state_manager(config.get_state_dir())
        plan_gen = create_plan_generator(state_manager, dep_graph)
        
        # Prepare model info
        model_info = {
            name: {
                'file_hash': pm.file_hash,
                'dependencies': list(pm.dependencies)
            }
            for name, pm in parsed_models.items()
        }
        
        # Generate plan
        models_list = models.split(',') if models else None
        exec_plan = plan_gen.generate_plan(model_info, models_list)
        
        # Display plan
        summary = exec_plan.get_summary()
        click.echo("\n=== Execution Plan ===")
        click.echo(f"Total models: {summary['total_models']}")
        click.secho(f"  Creates: {summary['creates']}", fg='green')
        click.secho(f"  Updates: {summary['updates']}", fg='yellow')
        click.secho(f"  Deletes: {summary['deletes']}", fg='red')
        click.echo(f"  No changes: {summary['no_changes']}")
        click.echo(f"Execution levels: {summary['execution_levels']}")
        
        click.echo("\n=== Changes ===")
        for change in exec_plan.changes:
            if change.change_type.value in ['create', 'update']:
                color = 'green' if change.change_type.value == 'create' else 'yellow'
                click.secho(
                    f"  [{change.change_type.value.upper()}] {change.model_name}: {change.reason}",
                    fg=color
                )
        
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        sys.exit(1)


@main.command()
@click.pass_context
def list(ctx):
    """List all models"""
    try:
        project_dir = ctx.obj['project_dir']
        config = load_config(project_dir)
        parser = SQLParser(config.get_models_dir())
        parsed_models = parser.parse_directory()
        
        click.echo(f"\n{len(parsed_models)} models found:\n")
        for name, pm in parsed_models.items():
            deps = ', '.join(pm.dependencies) if pm.dependencies else 'none'
            click.echo(f"  • {name} (dependencies: {deps})")
        
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')


@main.command()
@click.option('--format', default='text', help='Output format (text, graphviz)')
@click.pass_context
def deps(ctx, format):
    """Show dependency graph"""
    try:
        project_dir = ctx.obj['project_dir']
        config = load_config(project_dir)
        parser = SQLParser(config.get_models_dir())
        parsed_models = parser.parse_directory()
        dep_graph = build_dependency_graph(parsed_models)
        
        if format == 'graphviz':
            click.echo(dep_graph.export_graphviz())
        else:
            stats = dep_graph.get_stats()
            click.echo("\n=== Dependency Graph Statistics ===")
            click.echo(f"Total models: {stats['total_models']}")
            click.echo(f"Total edges: {stats['total_edges']}")
            click.echo(f"Max depth: {stats['max_depth']}")
            click.echo(f"Avg dependencies: {stats['avg_dependencies']}")
        
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')


@main.command()
@click.pass_context
def validate(ctx):
    """Validate all models"""
    try:
        project_dir = ctx.obj['project_dir']
        config = load_config(project_dir)
        parser = SQLParser(config.get_models_dir())
        
        click.echo("Validating models...")
        parsed_models = parser.parse_directory()
        
        errors = []
        for name, pm in parsed_models.items():
            try:
                parser.validate_sql(pm.parsed_sql)
            except Exception as e:
                errors.append((name, str(e)))
        
        if errors:
            click.secho(f"\n{len(errors)} validation errors:", fg='red')
            for name, error in errors:
                click.echo(f"  ✗ {name}: {error}")
            sys.exit(1)
        else:
            click.secho(f"\n✓ All {len(parsed_models)} models validated successfully", fg='green')
        
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        sys.exit(1)


@main.command()
@click.option('--watch', is_flag=True, help='Watch for file changes')
@click.pass_context
def serve(ctx, watch):
    """Start framework in server mode with hot reload"""
    try:
        project_dir = ctx.obj['project_dir']
        config = load_config(project_dir)
        
        if watch:
            def on_change(event_type, file_path):
                click.echo(f"File {event_type}: {file_path}")
                click.echo("Reloading...")
            
            watcher = create_file_watcher(config.get_models_dir(), on_change)
            watcher.start()
            
            click.echo("Watching for file changes... Press Ctrl+C to stop")
            try:
                while True:
                    pass
            except KeyboardInterrupt:
                watcher.stop()
                click.echo("\nStopped watching")
        
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        sys.exit(1)


if __name__ == '__main__':
    main()

