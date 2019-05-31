//-----------------------------------------------------------------------
// <copyright file="AccessControlType.cs" company="Microsoft">
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

using Microsoft.Azure.Storage.Core;
using System;

namespace Microsoft.Azure.Storage.Blob
{
    /// <summary>
    /// Specifies the type of the <see cref="PathAccessControlEntry"/>
    /// </summary>
    public enum AccessControlType
    {
        /// <summary>
        /// Specifies the <see cref="PathAccessControlEntry"/> applies to the owner or a named user.
        /// </summary>
        User,

        /// <summary>
        /// Specifies the <see cref="PathAccessControlEntry"/> applies to the owning group or a named group.
        /// </summary>
        Group,

        /// <summary>
        /// Specifies the <see cref="PathAccessControlEntry"/> sets a mask that restricts access to named users and member of groups.
        /// </summary>
        Mask,

        /// <summary>
        /// Specifies the <see cref="PathAccessControlEntry"/> applies to all users not found in other entries.
        /// </summary>
        Other
    }

    internal class AccessControlTypeHelper
    {
        internal static AccessControlType Parse(string typeString)
        {
            if("user".Equals(typeString.ToLowerInvariant()))
            {
                return AccessControlType.User;
            }
            else if("group".Equals(typeString.ToLowerInvariant()))
            {
                return AccessControlType.Group;
            }
            else if("mask".Equals(typeString.ToLowerInvariant()))
            {
                return AccessControlType.Mask;
            }
            else if("other".Equals(typeString.ToLowerInvariant()))
            {
                return AccessControlType.Other;
            }
            else
            {
                throw new ArgumentException(SR.UnidentifiedAccessControlType);
            }
        }
    }
}
