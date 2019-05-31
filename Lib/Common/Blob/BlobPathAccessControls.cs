//-----------------------------------------------------------------------
// <copyright file="BlobPathAccessControls.cs" company="Microsoft">
//    Copyright 2019 Microsoft Corporation
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Azure.Storage.Common.Blob
{
    using Microsoft.Azure.Storage.Blob;
    using System.Collections.Generic;

    /// <summary>
    /// Represents the properties for a CloudBlobPath
    /// </summary>
    public sealed class BlobPathAccessControls
    {
        /// <summary>
        /// Gets the owner of the blob path.
        /// </summary>
        public string Owner { get; set; }

        /// <summary>
        /// Gets the group of the blob path.
        /// </summary>
        public string Group { get; set; }

        /// <summary>
        /// Gets the permissions of the blob path.
        /// </summary>
        public PathPermissions Permissions { get; set; }

        /// <summary>
        /// Gets the ACL of the blob path.
        /// </summary>
        public IList<PathAccessControlEntry> ACL { get; set; }

        /// <summary>
        /// Public constructor.
        /// </summary>
        public BlobPathAccessControls() { }
    }
}
