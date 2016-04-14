from __future__ import absolute_import
from perftest import app

def monitor(app):
    state = app.events.State()

    def announce_task(kind):
        def announce_tasks(event):
            state.event(event)
            # task name is sent only with -received event, and state
            # will keep track of this for us.
            task = state.tasks.get(event['uuid'])

            print( kind + ': %s[%s] %s' % (
                task.name, task.uuid, task.info(), ))

        return announce_tasks

    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
            'task-sent': announce_task("task-sent"),
            'task-started': announce_task("task-started"),
            'task-succeeded': announce_task("task-succeeded"),
            'task-failed': announce_task("task-failed"),

        })
        recv.capture(limit=None, timeout=None, wakeup=True)

if __name__ == "__main__":
    monitor(app)
