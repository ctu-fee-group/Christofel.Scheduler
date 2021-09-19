//
//   RecoverableError.cs
//
//   Copyright (c) Christofel authors. All rights reserved.
//   Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Remora.Results;

namespace Christofel.Scheduling.Errors
{
    /// <summary>
    /// Error that should be returned from <see cref="IJobListener.BeforeExecutionAsync"/>
    /// </summary>
    /// <param name="Message">The message.</param>
    public record RecoverableError(string Message) : ResultError(Message);
}