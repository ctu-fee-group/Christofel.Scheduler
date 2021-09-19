//
//  BeforeExecutionError.cs
//
//  Copyright (c) Christofel authors. All rights reserved.
//  Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Remora.Results;

namespace Christofel.Scheduling.Errors
{
    /// <summary>
    /// Error that happens on begin execution events.
    /// </summary>
    /// <param name="Error">The underlying error.</param>
    public record BeforeExecutionError(IResultError Error) : ResultError(Error.Message);
}