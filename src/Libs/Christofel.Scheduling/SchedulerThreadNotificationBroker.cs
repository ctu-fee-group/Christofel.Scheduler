//
//  SchedulerThreadNotificationBroker.cs
//
//  Copyright (c) Christofel authors. All rights reserved.
//  Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using Nito.AsyncEx;

namespace Christofel.Scheduling
{
    /// <summary>
    /// Broker of notifications for <see cref="SchedulerThread"/>.
    /// </summary>
    public class SchedulerThreadNotificationBroker
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SchedulerThreadNotificationBroker"/> class.
        /// </summary>
        /// <param name="workResetEvent">The event to be set when there is a new notification.</param>
        public SchedulerThreadNotificationBroker(AsyncAutoResetEvent workResetEvent)
        {
            AddedJobs = new NotificationBroker<IJobDescriptor>(workResetEvent);
            ChangedJobs = new NotificationBroker<IJobDescriptor>(workResetEvent);
            ExecuteJobs = new NotificationBroker<IJobDescriptor>(workResetEvent);
            RemoveJobs = new NotificationBroker<JobKey>(workResetEvent);
        }

        /// <summary>
        /// Gets notification breaker for jobs that were added.
        /// </summary>
        public NotificationBroker<IJobDescriptor> AddedJobs { get; }

        /// <summary>
        /// Gets notification breaker for jobs that were changed.
        /// </summary>
        public NotificationBroker<IJobDescriptor> ChangedJobs { get; }

        /// <summary>
        /// Gets notification breaker for jobs that can be executed.
        /// </summary>
        public NotificationBroker<IJobDescriptor> ExecuteJobs { get; }

        /// <summary>
        /// Gets notification breaker for jobs that should be removed.
        /// </summary>
        public NotificationBroker<JobKey> RemoveJobs { get; }
    }
}