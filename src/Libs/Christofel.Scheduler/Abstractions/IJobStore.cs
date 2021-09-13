//
//   IJobStore.cs
//
//   Copyright (c) Christofel authors. All rights reserved.
//   Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Threading.Tasks;
using Remora.Results;

namespace Christofel.Scheduler.Abstractions
{
    /// <summary>
    /// Store for the jobs holding all the scheduled jobs.
    /// </summary>
    public interface IJobStore
    {
        /// <summary>
        /// Adds the specified job to the store.
        /// </summary>
        /// <param name="job">The data of the job to be added.</param>
        /// <param name="trigger">The trigger associated with the job that schedules execution of the job.</param>
        /// <returns>A result that may not have succeeded.</returns>
        public ValueTask<Result<IJobDescriptor>> AddJobAsync(IJobData job, ITrigger trigger);

        /// <summary>
        /// Removes the specified job from the store.
        /// </summary>
        /// <param name="jobKey">The key of the job to be removed.</param>
        /// <returns>A result that may not have succeeded.</returns>
        public ValueTask<Result> RemoveJobAsync(JobKey jobKey);

        /// <summary>
        /// Enumerates all of the jobs that are available.
        /// </summary>
        /// <returns>A result that may not have succeeded.</returns>
        public IReadOnlyList<IJobDescriptor> EnumerateJobs();
    }
}