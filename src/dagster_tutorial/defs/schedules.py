import dagster as dg


# Chosen as a daily schedule because the assignment describes a daily
# reporting pipeline with a fixed cadence.
@dg.schedule(cron_schedule="0 6 * * *", target="*")
def daily_sales_schedule(
    context: dg.ScheduleEvaluationContext,
) -> dg.RunRequest:
    return dg.RunRequest()