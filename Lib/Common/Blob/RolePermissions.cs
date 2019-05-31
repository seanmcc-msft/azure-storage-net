//-----------------------------------------------------------------------
// <copyright file="RolePermissions.cs" company="Microsoft">
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
    /// Represents file permissions for a specific role.
    /// </summary>
    public class RolePermissions : IEquatable<RolePermissions>
    {
        /// <summary>
        /// The read permission.
        /// </summary>
        public bool Read { get; set; }

        /// <summary>
        /// The write permission.
        /// </summary>
        public bool Write { get; set; }

        /// <summary>
        /// The execute permission.
        /// </summary>
        public bool Execute { get; set; }

        /// <summary>
        /// Internal empty constructor.
        /// </summary>
        public RolePermissions() { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="read">The read permission.</param>
        /// <param name="write">The write permission.</param>
        /// <param name="execute">The execute permission.</param>
        public RolePermissions(bool read, bool write, bool execute)
        {
            this.Read = read;
            this.Write = write;
            this.Execute = execute;
        }

        /// <summary>
        /// Parses octal char to RolePermissions.
        /// </summary>
        /// <param name="octal">Char to parse.</param>
        /// <returns><see cref="RolePermissions"/></returns>
        public static RolePermissions ParseOctal(char octal)
        {
            var rolePermissions = new RolePermissions();

            int value = Convert.ToInt32(octal);

            if(value < 0 || value > 7)
            {
                throw new ArgumentException(string.Format(SR.ArgumentOutOfRangeError, value));
            }

            if(value / 4 > 0)
            {
                rolePermissions.Read = true;
            }
            value %= 4;

            if(value / 2 > 0)
            {
                rolePermissions.Write = true;
            }
            value %= 2;

            if(value > 0)
            {
                rolePermissions.Execute = true;
            }

            return rolePermissions;
        }

        /// <summary>
        /// Parses symbolic permissions string to RolePermissions
        /// </summary>
        /// <param name="str">String to parse</param>
        /// <param name="allowStickyBit">If sticky bit is allowed</param>
        /// <returns><see cref="RolePermissions"/></returns>
        public static RolePermissions ParseSymbolic(string str, bool allowStickyBit)
        {
            var rolePermissions = new RolePermissions();

            if(str == null)
            {
                throw new NullReferenceException(SR.NullArguementError);
            }

            if(str.Length < 3)
            {
                throw new ArgumentException(SR.ArgumentTooSmallError);
            }

            if(str.Length > 3)
            {
                throw new ArgumentException(SR.ArgumentTooLargeError);
            }

            var argException = new ArgumentException(SR.ArgumentFormatError);

            if(str[0] == 'r')
            {
                rolePermissions.Read = true;
            }
            else if(str[0] != '-')
            {
                throw argException;
            }

            if(str[1] == 'w')
            {
                rolePermissions.Write = true;
            }
            else if(str[1] != '-')
            {
                throw argException;
            }

            if(str[2] == 'x')
            {
                rolePermissions.Execute = true;
            }
            else if(allowStickyBit)
            {
                if(str[2] == 't')
                {
                    rolePermissions.Execute = true;
                }
                else if(str[2] != 'T' && str[2] != '-')
                {
                    throw argException;
                }
            }
            else if(str[2] != '-')
            {
                throw argException;
            }

            return rolePermissions;
        }

        /// <summary>
        /// Returns the octal string representation of this RolePermissions.
        /// </summary>
        /// <returns>String</returns>
        public string ToOctalString()
        {
            return ToOctal().ToString();
        }

        private int ToOctal()
        {
            int res = 0;

            if (this.Read)
            {
                res |= (1 << 2);
            }

            if (this.Write)
            {
                res |= (1 << 1);
            }

            if (this.Execute)
            {
                res |= 1;
            }

            return res;
        }

        /// <summary>
        /// Returns the octal string respentation of this RolePermissions.
        /// </summary>
        /// <returns>String</returns>
        public string ToSymbolicString()
        {
            var symbolicString = "";

            symbolicString += this.Read ? "r" : "-";
            symbolicString += this.Write ? "w" : "-";
            symbolicString += this.Execute ? "x" : "-";

            return symbolicString;
        }

        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool Equals(RolePermissions other)
        {
            if(other != null
                && this.Read == other.Read
                && this.Write == other.Write
                && this.Execute == other.Execute)
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
        public override bool Equals(object other)
        {
            return other is RolePermissions && this.Equals((RolePermissions)other);
        }

        /// <summary>
        /// Overrides GetHashCode()
        /// </summary>
        /// <returns>int</returns>
        public override int GetHashCode()
        {
            return ToOctal();
        }
    }
}
