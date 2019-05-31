//-----------------------------------------------------------------------
// <copyright file="AccessControl.cs" company="Microsoft">
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
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Microsoft.Azure.Storage.Blob
{
    /// <summary>
    /// Represents an access control in a file ACL
    /// </summary>
    public class PathAccessControlEntry : IEquatable<PathAccessControlEntry>
    {
        /// <summary>
        /// Indicates whether this is the default entry for the ACL.
        /// </summary>
        public bool DefaultScope { get; set; }

        /// <summary>
        /// Specifies which role this entry targets.
        /// </summary>
        public AccessControlType? AccessControlType { get; set; }

        /// <summary>
        /// Specifies the entity for which this entry applies.
        /// 
        /// Must be omitted for types mask or other.  It must also be omitted when the user or group is the owner.
        /// </summary>
        public string EntityId { get; set; }

        /// <summary>
        /// Specifies the permissions granted to this entry.
        /// </summary>
        public RolePermissions Permissions { get; set; }

        /// <summary>
        /// Empty constructor.
        /// </summary>
        public PathAccessControlEntry() { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="accessControlType">Specifies which role this entry targets.</param>
        /// <param name="permissions">Specifies the permissions granted to this entry.</param>
        /// <param name="defaultScope">Indicates whether this is the default entry for the ACL.</param>
        /// <param name="entityId">Specifies the entity for which this entry applies.</param>
        public PathAccessControlEntry(
            AccessControlType accessControlType,
            RolePermissions permissions,
            bool defaultScope = false,
            string entityId = null)
        {
            this.DefaultScope = defaultScope;
            this.AccessControlType = accessControlType;
            this.EntityId = entityId;
            this.Permissions = permissions;
        }

        /// <summary>
        /// Override of ToString()
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            if(this.AccessControlType == null)
            {
                string errorMessage = string.Format(CultureInfo.CurrentCulture, SR.NullProperty, "AccessControlType");
            }

            if(this.Permissions == null)
            {
                string errorMessage = string.Format(CultureInfo.CurrentCulture, SR.NullProperty, "Permissions");
            }

            StringBuilder sb = new StringBuilder();

            if(this.DefaultScope)
            {
                sb.Append("default:");
            }
            sb.Append(AccessControlType.Value.ToString().ToLowerInvariant());
            sb.Append(":");
            sb.Append(EntityId ?? "");
            sb.Append(":");
            sb.Append(Permissions.ToSymbolicString());

            return sb.ToString();
        }

        /// <summary>
        /// Parses the provided string into a <see cref="PathAccessControlEntry"/>
        /// </summary>
        /// <param name="str">The string representation of the ACL.</param>
        /// <returns>A <see cref="PathAccessControlEntry"/></returns>
        public static PathAccessControlEntry Parse(string str)
        {
            PathAccessControlEntry entry = new PathAccessControlEntry();
            string[] parts = str.Split(':');
            int indexOffset = 0;

            if(parts.Length == 4)
            {
                if(!parts[0].Equals("default"))
                {
                    throw new ArgumentException(SR.AccessControlEntryInvalidScope);
                }
                entry.DefaultScope = true;
                indexOffset = 1;
            }
            entry.AccessControlType = AccessControlTypeHelper.Parse(parts[indexOffset]);

            if(parts[1 + indexOffset] != "")
            {
                entry.EntityId = parts[1 + indexOffset];
            }

            entry.Permissions = RolePermissions.ParseSymbolic(parts[2 + indexOffset], false);
            return entry;
        }

        /// <summary>
        /// Converts the Access Control List to a <see cref="string"/>.
        /// </summary>
        /// <param name="acl">The Access Control List to serialize</param>
        /// <returns>string</returns>
        public static string SerializeList(IList<PathAccessControlEntry> acl)
        {
            IList<string> serializedAcl = new List<string>();
            foreach(var ac in acl)
            {
                serializedAcl.Add(ac.ToString());
            }
            return string.Join(",", serializedAcl);
        }

        internal static IList<PathAccessControlEntry> ParseList(string str)
        {
            string[] strs = str.Split(',');
            List<PathAccessControlEntry> acl = new List<PathAccessControlEntry>();
            foreach(string entry in strs)
            {
                acl.Add(Parse(entry));
            }
            return acl;
        }

        /// <summary>
        /// Override Equals()
        /// </summary>
        /// <param name="other">Another <see cref="PathAccessControlEntry"/></param>
        /// <returns></returns>
        public bool Equals(PathAccessControlEntry other)
        {
            if (other != null
                && this.DefaultScope == other.DefaultScope
                && this.AccessControlType == other.AccessControlType
                && this.EntityId == other.EntityId
                && this.Permissions.Equals(other.Permissions))
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Override Equals
        /// </summary>
        /// <param name="other">An <see cref="object"/></param>
        /// <returns></returns>
        public override bool Equals(object other)
        {
            return other is PathAccessControlEntry && this.Equals((PathAccessControlEntry)other);
        }
    }
}
