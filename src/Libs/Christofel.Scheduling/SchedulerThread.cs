//
//   SchedulerThread.cs
//
//   Copyright (c) Christofel authors. All rights reserved.
//   Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Christofel.Scheduling.Errors;
using Christofel.Scheduling.Extensions;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;
using Remora.Results;

namespace Christofel.Scheduling
{
    /// <summary>
    /// Executes jobs from the storage.
    /// </summary>
    public class SchedulerThread
    {
        private static readonly TimeSpan _getJobsTillTimespan = TimeSpan.FromMinutes(30);

        private readonly IJobStore _jobStore;
        private readonly ILogger _logger;
        private readonly IJobExecutor _executor;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly AsyncAutoResetEvent _workResetEvent;
        private readonly HashSet<JobKey> _standbyJobs;

        /// <summary>
        /// Initializes a new instance of the <see cref="SchedulerThread"/> class.
        /// </summary>
        /// <param name="jobStore">The storage for the jobs.</param>
        /// <param name="logger">The logger.</param>
        /// <param name="executor">The executor that executes the given jobs.</param>
        public SchedulerThread
        (
            IJobStore jobStore,
            ILogger<SchedulerThread> logger,
            IJobExecutor executor
        )
        {
            _jobStore = jobStore;
            _logger = logger;
            _executor = executor;
            _cancellationTokenSource = new CancellationTokenSource();

            _standbyJobs = new HashSet<JobKey>();
            _workResetEvent = new AsyncAutoResetEvent();
            NotificationBroker = new SchedulerThreadNotificationBroker(_workResetEvent);
        }

        /// <summary>
        /// Gets the broker of notifications.
        /// </summary>
        public SchedulerThreadNotificationBroker NotificationBroker { get; }

        /// <summary>
        /// Starts the scheduler thread.
        /// </summary>
        public void Start()
        {
            Task.Run
            (
                async () =>
                {
                    try
                    {
                        await Run();
                    }
                    catch (Exception e)
                    {
                        _logger.LogCritical
                        (
                            e,
                            "There was an exception inside of SchedulerThread. The Scheduler won't work correctly"
                        );
                    }
                }
            );
        }

        /// <summary>
        /// Exists the scheduler thread.
        /// </summary>
        public void Stop()
        {
            _workResetEvent.Set();
            _cancellationTokenSource.Cancel();
        }

        /// <summary>
        /// Processes remaining jobs until stop is called.
        /// </summary>
        private async Task Run()
        {
            while (!_cancellationTokenSource.IsCancellationRequested)
            {
                var till = DateTimeOffset.UtcNow.Add(_getJobsTillTimespan);

                await ClearNotificationsAsync(); // Notifications are not needed when pulling the jobs from the store.
                var list = await _jobStore.GetJobsTillAsync(till);

                var queue = new PriorityQueue<IJobDescriptor, DateTimeOffset>();
                for (var i = 0; i < list.Count; i++)
                {
                    queue.Enqueue(list[i], list[i].Trigger.NextFireDate ?? DateTimeOffset.UtcNow);
                }

                list = null;

                // Should block until "till" time is reached.
                await ProcessQueueTillAsync(queue, till, _cancellationTokenSource.Token);
            }
        }

        private async Task ClearNotificationsAsync()
        {
            await NotificationBroker.AddedJobs.ClearNotificationsAsync();
            await NotificationBroker.ChangedJobs.ClearNotificationsAsync();
            await NotificationBroker.ExecuteJobs.ClearNotificationsAsync();
            await NotificationBroker.RemoveJobs.ClearNotificationsAsync();
        }

        /// <summary>
        /// Processes the given queue and listens to the notifications until <paramref name="till"/> is reached.
        /// </summary>
        /// <param name="queue">The queue of the jobs that are stored until the <paramref name="till"/>.</param>
        /// <param name="till">The time till this method should block and process both the queue and notifications.</param>
        private async Task ProcessQueueTillAsync
            (PriorityQueue<IJobDescriptor, DateTimeOffset> queue, DateTimeOffset till, CancellationToken ct)
        {
            // Token that cancels either after "till" is reached or "ct" is canceled.
            using var delayedCancellationToken = new CancellationTokenSource();
            delayedCancellationToken.CancelAfter(_getJobsTillTimespan);
            using var connectedTokenSource = CancellationTokenSource.CreateLinkedTokenSource
                (delayedCancellationToken.Token, ct);

            while (!ct.IsCancellationRequested || DateTimeOffset.UtcNow <= till)
            {
                if (queue.Count == 0)
                {
                    var notificationsInterrupt = await _workResetEvent.WaitSafeAsync(connectedTokenSource.Token);

                    if (!notificationsInterrupt)
                    {
                        break; // "till" time was reached, the global Run should take care of things.
                    }

                    // Notifications have interrupted, so we check them.
                    await CheckNotificationsAsync(queue, till, ct);
                }

                while (queue.Count > 0)
                {
                    try
                    {
                        if (!queue.TryPeek(out var job, out _))
                        {
                            break; // The condition of the while should not be met, but ... just to be sure, you know :D
                        }

                        // Stand by jobs is used for currently executing jobs and for jobs that have to wait for execution
                        if (_standbyJobs.Contains(job.Key))
                        {
                            queue.Dequeue();
                            continue;
                        }

                        var result = await ProcessJobAsync(job, _cancellationTokenSource.Token);
                        var shouldRemove = false;
                        if (result.IsSuccess)
                        {
                            switch (result.Entity)
                            {
                                case JobExecuteState.NotificationsInterrupt:
                                    await CheckNotificationsAsync(queue, till, _cancellationTokenSource.Token);
                                    break;
                                case JobExecuteState.None:
                                    queue.Dequeue();
                                    break;
                                case JobExecuteState.RegisteredReadyCallback:
                                case JobExecuteState.Executing:
                                    _standbyJobs.Add(job.Key);
                                    queue.Dequeue();
                                    break;
                            }
                        }
                        else
                        {
                            _logger.LogResult(result, $"Could not execute job {job.Key}, the job will be removed from the queue and store.");
                            queue.Dequeue();
                            shouldRemove = true;
                        }

                        if (shouldRemove || job.Trigger.NextFireDate is null)
                        {
                            var removeResult = await _jobStore.RemoveJobAsync(job.Key);
                            if (!removeResult.IsSuccess)
                            {
                                _logger.LogResult(removeResult, $"Could not remove job {job.Key} from the store");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        _logger.LogWarning("Encountered an operation canceled exception");
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e, "An exception was thrown in the scheduler thread");
                    }
                }
            }
        }

        /// <summary>
        /// Processes the given job, returning what was done.
        /// </summary>
        /// <param name="job">The job to be processed.</param>
        /// <param name="ct">The cancellation token for the operation.</param>
        private async Task<Result<JobExecuteState>> ProcessJobAsync(IJobDescriptor job, CancellationToken ct)
        {
            var fireTime = job.Trigger.NextFireDate;
            bool notificationInterrupt = false;

            // The job is somewhere in the future, wait for it
            // and wait for notifications.
            if (fireTime is not null && fireTime > DateTimeOffset.UtcNow)
            {
                using var delayedCancellationToken = new CancellationTokenSource();
                using var connectedCancellationToken = CancellationTokenSource.CreateLinkedTokenSource
                    (delayedCancellationToken.Token, ct);
                delayedCancellationToken.CancelAfter((DateTimeOffset)fireTime - DateTimeOffset.UtcNow);
                notificationInterrupt = await _workResetEvent.WaitSafeAsync(connectedCancellationToken.Token);
            }
            else if (fireTime is null)
            {
                return JobExecuteState.None;
            }

            if (notificationInterrupt)
            {
                // The job is still in the future, notifications have come.
                return JobExecuteState.NotificationsInterrupt;
            }
            else if (!await job.Trigger.CanBeExecutedAsync())
            {
                await job.Trigger.RegisterReadyCallbackAsync
                (
                    async () =>
                    {
                        await NotificationBroker.ExecuteJobs.NotifyAsync((job, Result.FromSuccess()), ct);
                    }
                );

                return JobExecuteState.RegisteredReadyCallback;
            }
            else
            {
                var beginExecutionResult = await _executor.BeginExecutionAsync
                (
                    job,
                    async (returnJob, result) =>
                    {
                        await NotificationBroker.ExecuteJobs.NotifyAsync((returnJob, result), ct);
                    },
                    ct
                );

                if (!beginExecutionResult.IsSuccess)
                {
                    return Result<JobExecuteState>.FromError(beginExecutionResult);
                }

                return JobExecuteState.Executing;
            }
        }

        private async Task CheckExecuteJobsNotificationsAsync
            (PriorityQueue<IJobDescriptor, DateTimeOffset> enqueuedJobs, DateTimeOffset till, CancellationToken ct)
        {
            if (await NotificationBroker.ExecuteJobs.HasPendingNotifications(ct))
            {
                (IDisposable @lock, Queue<(IJobDescriptor Job, Result Result)> jobData) =
                    await NotificationBroker.ExecuteJobs.GetNotifications(ct);
                using (@lock)
                {
                    while (jobData.TryDequeue(out var data))
                    {
                        _standbyJobs.Remove(data.Job.Key);

                        if (!data.Result.IsSuccess && data.Result.Error is BeforeExecutionError beginExecutionError)
                        {
                            if (beginExecutionError.Error is RecoverableError recoverableError)
                            {
                                _logger.LogError
                                (
                                    "A recoverable error has happened during begin execution event, the job will be added to the execution queue. {Error}",
                                    recoverableError.Message
                                );
                                enqueuedJobs.Enqueue(data.Job, data.Job.Trigger.NextFireDate ?? DateTimeOffset.UtcNow);
                            }
                            else
                            {
                                _logger.LogError
                                (
                                    "An error has occurred during before execution events, the job won't be executed and will be removed from the execution queue. {Error}",
                                    beginExecutionError.Message
                                );
                                var removeResult = await _jobStore.RemoveJobAsync(data.Job.Key);
                                if (!removeResult.IsSuccess)
                                {
                                    _logger.LogResult(removeResult, $"Could not remove job {data.Job.Key}");
                                }
                            }
                        }
                        else if (data.Job.Trigger.NextFireDate <= till)
                        {
                            enqueuedJobs.Enqueue(data.Job, data.Job.Trigger.NextFireDate ?? DateTimeOffset.UtcNow);
                        }
                    }
                }
            }
        }

        private async Task CheckAddedJobsNotificationsAsync
        (
            PriorityQueue<IJobDescriptor, DateTimeOffset> enqueuedJobs,
            DateTimeOffset till,
            CancellationToken ct
        )
        {
            if (await NotificationBroker.AddedJobs.HasPendingNotifications(ct))
            {
                (IDisposable @lock, Queue<IJobDescriptor> jobs) =
                    await NotificationBroker.AddedJobs.GetNotifications(ct);
                using (@lock)
                {
                    while (jobs.TryDequeue(out var job))
                    {
                        enqueuedJobs.Enqueue(job, job.Trigger.NextFireDate ?? DateTimeOffset.UnixEpoch);
                    }
                }
            }
        }

        private async Task CheckChangedJobsNotificationsAsync
        (
            Dictionary<JobKey, IJobDescriptor?> changeJobs,
            PriorityQueue<IJobDescriptor, DateTimeOffset> enqueuedJobs,
            DateTimeOffset till,
            CancellationToken ct
        )
        {
            if (await NotificationBroker.ChangedJobs.HasPendingNotifications(ct))
            {
                (IDisposable @lock, Queue<IJobDescriptor> jobs) =
                    await NotificationBroker.ChangedJobs.GetNotifications(ct);
                using (@lock)
                {
                    while (jobs.TryDequeue(out var job))
                    {
                        IJobDescriptor? storedJob = null;
                        foreach (var x in enqueuedJobs.UnorderedItems)
                        {
                            var x1 = x.Element;
                            if (x1.Key == job.Key)
                            {
                                storedJob = x1;
                                break;
                            }
                        }

                        if (storedJob is null)
                        {
                            if (job.Trigger.NextFireDate < till)
                            {
                                enqueuedJobs.Enqueue(job, job.Trigger.NextFireDate ?? DateTimeOffset.UnixEpoch);
                            }
                        }
                        else if (storedJob != job)
                        {
                            changeJobs[job.Key] = job.Trigger.NextFireDate > till
                                ? null
                                : job;
                        }
                    }
                }
            }
        }

        private async Task CheckRemoveJobsNotificationsAsync
        (
            Dictionary<JobKey, IJobDescriptor?> changeJobs,
            PriorityQueue<IJobDescriptor, DateTimeOffset> enqueuedJobs,
            DateTimeOffset till,
            CancellationToken ct
        )
        {
            if (await NotificationBroker.RemoveJobs.HasPendingNotifications(ct))
            {
                (IDisposable @lock, Queue<JobKey> jobs) = await NotificationBroker.RemoveJobs.GetNotifications(ct);
                using (@lock)
                {
                    while (jobs.TryDequeue(out var jobKey))
                    {
                        _standbyJobs.Remove(jobKey);

                        var containsJob = false;
                        foreach (var x in enqueuedJobs.UnorderedItems)
                        {
                            if (x.Element.Key == jobKey)
                            {
                                containsJob = true;
                                break;
                            }
                        }

                        if (containsJob)
                        {
                            changeJobs[jobKey] = null;
                        }
                    }
                }
            }
        }

        private async Task CheckNotificationsAsync
            (PriorityQueue<IJobDescriptor, DateTimeOffset> enqueuedJobs, DateTimeOffset till, CancellationToken ct)
        {
            var changeJobs = new Dictionary<JobKey, IJobDescriptor?>();

            await CheckExecuteJobsNotificationsAsync(enqueuedJobs, till, ct);
            await CheckAddedJobsNotificationsAsync(enqueuedJobs, till, ct);
            await CheckChangedJobsNotificationsAsync(changeJobs, enqueuedJobs, till, ct);
            await CheckRemoveJobsNotificationsAsync(changeJobs, enqueuedJobs, till, ct);

            if (changeJobs.Count != 0)
            {
                var storedDequeuedList = new List<IJobDescriptor>();
                while (changeJobs.Count > 0)
                {
                    if (enqueuedJobs.TryDequeue(out var storedJob, out _))
                    {
                        if (changeJobs.ContainsKey(storedJob.Key))
                        {
                            var swapJob = changeJobs[storedJob.Key];
                            if (swapJob is not null)
                            {
                                enqueuedJobs.Enqueue(swapJob, swapJob.Trigger.NextFireDate ?? DateTimeOffset.UnixEpoch);
                            }

                            changeJobs.Remove(storedJob.Key);
                        }
                        else
                        {
                            storedDequeuedList.Add(storedJob);
                        }
                    }
                }

                foreach (var storedDequeued in storedDequeuedList)
                {
                    enqueuedJobs.Enqueue
                    (
                        storedDequeued,
                        storedDequeued.Trigger.NextFireDate ?? DateTimeOffset.UnixEpoch
                    );
                }

                storedDequeuedList.Clear();
            }
        }

        private enum JobExecuteState
        {
            /// <summary>
            /// There was nothing done, the job should probably be removed.
            /// </summary>
            None,

            /// <summary>
            /// The job was not executed, the notifications were fired.
            /// </summary>
            NotificationsInterrupt,

            /// <summary>
            /// The job started executing successfully.
            /// </summary>
            Executing,

            /// <summary>
            /// The job has registered a ready callback that will be called when the job is ready,
            /// calling the notification ExecuteJobs.
            /// </summary>
            RegisteredReadyCallback,
        }
    }
}