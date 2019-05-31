//-----------------------------------------------------------------------
// <copyright file="PathPermissions.cs" company="Microsoft">
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
using System.Text;

namespace Microsoft.Azure.Storage.Blob
{
    /// <summary>
    /// Represents POSIX-style permissions on a given resource. Each resource specifies permissions for the owner, the owning
    /// group, and everyone else. Permissions for users or groups not included here can be set using an Access Control List.
    /// Manipulating resource permissions is only supported when ADLS interop is enabled.
    /// </summary>
    public class PathPermissions : IEquatable<PathPermissions>
    {
        /// <summary>
        /// The <see cref="RolePermissions"/> for the owner of the resource.
        /// </summary>
        public RolePermissions Owner { get; set; }

        /// <summary>
        /// The <see cref="RolePermissions"/> for the owning group of the resource.
        /// </summary>
        public RolePermissions Group { get; set; }

        /// <summary>
        /// The <see cref="RolePermissions"/> for the other users.
        /// </summary>
        public RolePermissions Other { get; set; }

        /// <summary>
        /// If the sticky bit has been set. The sticky bit may be set on directories, the files in that
        /// directory may only be renamed or deleted by the file's owner, the directory's owner, or the root user.
        /// </summary>
        public bool StickyBit { get; set; }

        /// <summary>
        /// Whether or not there is more permissions information in the ACLs. The permissions string only returns
        /// information on the owner, owning group, and other, but the ACLs may contain more permissions for specific users
        /// or groups.
        /// </summary>
        public bool ExtendedInfoInAcl { get; set; }

        /// <summary>
        /// Internal empty constructor.
        /// </summary>
        public PathPermissions() { }
   
        /// <summary>
        /// Public constructor.
        /// </summary>
        /// <param name="owner">The path owner's permissions.</param>
        /// <param name="group">The path group's permissions.</param>
        /// <param name="other">Permissions for other users.</param>
        /// <param name="stickyBit">If sticky bit is enabled</param>
        /// <param name="extendedInfoInAcl">If there is extended info in the ACL</param>
        public PathPermissions(RolePermissions owner, RolePermissions group, RolePermissions other, bool stickyBit, bool extendedInfoInAcl)
        {
            this.Owner = owner;
            this.Group = group;
            this.Other = other;
            this.StickyBit = stickyBit;
            this.ExtendedInfoInAcl = extendedInfoInAcl;
        }

        /// <summary>
        /// Parses a string in octal format to PathPermissions.
        /// </summary>
        /// <param name="octal">Octal string to parse.</param>
        /// <returns><see cref="PathPermissions"/></returns>
        public static PathPermissions ParseOctal(string octal)
        {
            if (octal == null)
            {
                throw new NullReferenceException(SR.NullArguementError);
            }

            if(octal.Length != 4)
            {
                throw new ArgumentException(string.Format(SR.ArgumentOutOfRangeError, octal));
            }

            var pathPermissions = new PathPermissions();

            if(octal[0] == '0')
            {
                pathPermissions.StickyBit = false;
            }
            else
            {
                pathPermissions.StickyBit = true;
            }

            pathPermissions.Owner = RolePermissions.ParseOctal(octal[1]);
            pathPermissions.Group = RolePermissions.ParseOctal(octal[2]);
            pathPermissions.Owner = RolePermissions.ParseOctal(octal[3]);

            return pathPermissions;
        }

        /// <summary>
        /// Parses a symbolic string to PathPermissions.
        /// </summary>
        /// <param name="str">String to parse</param>
        /// <returns><see cref="PathPermissions"/></returns>
        public static PathPermissions ParseSymbolic(string str)
        {
            if(str == null)
            {
                throw new NullReferenceException(SR.NullArguementError);
            }

            if(str.Length != 9 && str.Length != 10)
            {
                throw new ArgumentException(string.Format(SR.ArgumentOutOfRangeError, str));
            }

            var pathPermissions = new PathPermissions();

            // Set sticky bit
            if(str.ToLower()[str.Length - 1] == 't')
            {
                pathPermissions.StickyBit = true;
            }
            else
            {
                pathPermissions.StickyBit = false;
            }

            // Set extended info in ACL
            if(str.Length == 10)
            {
                if(str[9] == '+')
                {
                    pathPermissions.ExtendedInfoInAcl = true;
                }
                else
                {
                    throw new ArgumentException(SR.ArgumentFormatError);
                }
            }
            else
            {
                pathPermissions.ExtendedInfoInAcl = false;
            }

            pathPermissions.Owner = RolePermissions.ParseSymbolic(str.Substring(0, 3), allowStickyBit: false);
            pathPermissions.Group = RolePermissions.ParseSymbolic(str.Substring(3, 3), allowStickyBit: false);
            pathPermissions.Other = RolePermissions.ParseSymbolic(str.Substring(6, 3), allowStickyBit: true);

            return pathPermissions;
        }

        /// <summary>
        /// Returns the octal representation of this PathPermissions as a string.
        /// </summary>
        /// <returns>string</returns>
        public string ToOctalString()
        {
            var sb = new StringBuilder();

            if(this.StickyBit)
            {
                sb.Append(1);
            }
            else
            {
                sb.Append(0);
            }

            sb.Append(this.Owner.ToOctalString());
            sb.Append(this.Group.ToOctalString());
            sb.Append(this.Other.ToOctalString());

            return sb.ToString();
        }

        /// <summary>
        /// Returns the symbolic represenation of this PathPermissions as a string. 
        /// </summary>
        /// <returns>string</returns>
        public string ToSymbolicString()
        {
            var sb = new StringBuilder();
            sb.Append(this.Owner.ToSymbolicString());
            sb.Append(this.Group.ToSymbolicString());
            sb.Append(this.Other.ToSymbolicString());

            if(this.StickyBit)
            {
                sb.Remove(8, 1);
                sb.Append("t");
            }

            return sb.ToString();
        }

        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool Equals(PathPermissions other)
        {
            if(other != null
                && this.Owner.Equals(other.Owner)
                && this.Group.Equals(other.Group)
                && this.Other.Equals(other.Other)
                && this.StickyBit == other.StickyBit)
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public override bool Equals(Object other)
        {
            return other is PathPermissions && this.Equals((PathPermissions)other);
        }
    }
}
