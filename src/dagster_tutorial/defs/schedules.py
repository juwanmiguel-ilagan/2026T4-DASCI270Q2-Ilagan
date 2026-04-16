import dagster as dg


# Chosen as a daily schedule because the pipeline is meant to produce
# recurring daily sales summaries at a fixed cadence.
@dg.schedule(cron_schedule="0 6 * * *", target="*")
def daily_sales_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.RunRequest:
    return dg.RunRequest()