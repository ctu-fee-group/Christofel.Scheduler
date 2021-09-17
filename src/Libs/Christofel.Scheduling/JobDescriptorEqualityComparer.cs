//
//   JobDescriptorEqualityComparer.cs
//
//   Copyright (c) Christofel authors. All rights reserved.
//   Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;

namespace Christofel.Scheduling
{
    /// <summary>
    /// Compares <see cref="IJobDescriptor"/> by the <see cref="JobKey"/>.
    /// </summary>
    public class JobDescriptorEqualityComparer : IEqualityComparer<IJobDescriptor>
    {
        /// <inheritdoc />
        public bool Equals(IJobDescriptor? x, IJobDescriptor? y) => x?.Key == y?.Key;

        /// <inheritdoc />
        public int GetHashCode(IJobDescriptor obj) => obj.Key.GetHashCode();
    }
}