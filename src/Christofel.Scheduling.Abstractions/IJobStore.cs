//
//   IJobStore.cs
//
//   Copyright (c) Christofel authors. All rights reserved.
//   Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Remora.Results;

namespace Christofel.Scheduling
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
        /// Returns whether job with the specified key exists in the store.
        /// </summary>
        /// <param name="jobKey">The key of the job to be removed.</param>
        /// <returns>A result that may not have succeeded.</returns>
        public ValueTask<Result<bool>> HasJobAsync(JobKey jobKey);

        /// <summary>
        /// Gets the specified job from the store.
        /// </summary>
        /// <param name="jobKey">The key of the job to be removed.</param>
        /// <returns>A result that may not have succeeded.</returns>
        public ValueTask<Result<IJobDescriptor>> GetJobAsync(JobKey jobKey);

        /// <summary>
        /// Returns all the job that should fire till the specified date.
        /// </summary>
        /// <param name="till">The date till to find jobs.</param>
        /// <returns>A result that may not have succeeded.</returns>
        public ValueTask<IReadOnlyList<IJobDescriptor>> GetJobsTillAsync(DateTimeOffset till);

        /// <summary>
        /// Enumerates all of the jobs that are available.
        /// </summary>
        /// <returns>A result that may not have succeeded.</returns>
        public IReadOnlyCollection<IJobDescriptor> GetAllJobs();
    }
}