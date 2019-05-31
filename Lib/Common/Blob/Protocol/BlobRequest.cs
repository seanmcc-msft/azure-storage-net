//-----------------------------------------------------------------------
// <copyright file="BlobRequest.cs" company="Microsoft">
//    Copyright 2013 Microsoft Corporation
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

namespace Microsoft.Azure.Storage.Blob.Protocol
{
    using Microsoft.Azure.Storage.Core;
    using Microsoft.Azure.Storage.Core.Auth;
    using Microsoft.Azure.Storage.Core.Util;
    using Microsoft.Azure.Storage.Shared.Protocol;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Text;
    using System.Xml;

    /// <summary>
    /// Provides a set of helper methods for constructing a request against the Blob service.
    /// </summary>
#if WINDOWS_RT
    internal
#else
    public
#endif
        static class BlobRequest
    {
        /// <summary>
        /// Writes a collection of shared access policies to the specified stream in XML format.
        /// </summary>
        /// <param name="sharedAccessPolicies">A collection of shared access policies.</param>
        /// <param name="outputStream">An output stream.</param>
        public static void WriteSharedAccessIdentifiers(SharedAccessBlobPolicies sharedAccessPolicies, Stream outputStream)
        {
            Request.WriteSharedAccessIdentifiers(
                sharedAccessPolicies,
                outputStream,
                (policy, writer) =>
                {
                    writer.WriteElementString(
                        Constants.Start,
                        SharedAccessSignatureHelper.GetDateTimeOrEmpty(policy.SharedAccessStartTime));
                    writer.WriteElementString(
                        Constants.Expiry,
                        SharedAccessSignatureHelper.GetDateTimeOrEmpty(policy.SharedAccessExpiryTime));
                    writer.WriteElementString(
                        Constants.Permission,
                        SharedAccessBlobPolicy.PermissionsToString(policy.Permissions));
                });
        }

        /// <summary>
        /// Writes the body of the block list to the specified stream in XML format.
        /// </summary>
        /// <param name="blocks">An enumerable collection of <see cref="PutBlockListItem"/> objects.</param>
        /// <param name="outputStream">The stream to which the block list is written.</param>
        public static void WriteBlockListBody(IEnumerable<PutBlockListItem> blocks, Stream outputStream)
        {
            CommonUtility.AssertNotNull("blocks", blocks);

            XmlWriterSettings settings = new XmlWriterSettings();
            settings.Encoding = Encoding.UTF8;
            using (XmlWriter writer = XmlWriter.Create(outputStream, settings))
            {
                writer.WriteStartElement(Constants.BlockListElement);

                foreach (PutBlockListItem block in blocks)
                {
                    if (block.SearchMode == BlockSearchMode.Committed)
                    {
                        writer.WriteElementString(Constants.CommittedElement, block.Id);
                    }
                    else if (block.SearchMode == BlockSearchMode.Uncommitted)
                    {
                        writer.WriteElementString(Constants.UncommittedElement, block.Id);
                    }
                    else if (block.SearchMode == BlockSearchMode.Latest)
                    {
                        writer.WriteElementString(Constants.LatestElement, block.Id);
                    }
                }

                writer.WriteEndDocument();
            }
        }

        /// <summary>
        /// Adds metadata in the format accepted by ADLSGen2 apis: " a comma-separated list of name and value pairs
        /// "n1=v1, n2=v2, ...", where each value is a base64 encoded string. Note that the string may only contain ASCII
        /// characters in the ISO-8859-1 character set."
        /// </summary>
        /// <param name="metadata">The metadata</param>
        public static string MetadataAsPathProperties(IDictionary<string, string> metadata)
        {
            if (metadata == null || metadata.Count == 0)
            {
                return null;
            }

            StringBuilder sb = new StringBuilder();

            foreach (KeyValuePair<string, string> entry in metadata)
            {
                CommonUtility.AssertNotNull("value", entry.Value);
                if (string.IsNullOrWhiteSpace(entry.Value))
                {
                    throw new ArgumentException(SR.ArgumentEmptyError, entry.Value);
                }

                /*
                The service has an internal base64 decode when metadata is copied from ADLS to Storage, so getMetadata
                will work as normal. Doing this encoding for the customers preserves the existing behavior of
                metadata.
                 */
                sb.Append(entry.Key).Append('=').Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(entry.Value))).Append(",");
            }

            // Remove trailing comma
            sb.Remove(sb.Length - 1, 1);

            return sb.ToString();
        }

        /// <summary>
        /// Returns a new Uri with ".blob." replaced with ".dfs." in the host,
        /// </summary>
        /// <param name="uri">Inital Uri</param>
        /// <returns>New Uri with host swapped</returns>
        public static Uri SwapDfsEndpoint(Uri uri)
        {
            // Remove trailing delimier
            uri = RemoveTrailingDelimiter(uri);

            // Swap to dfs endpoint
            string newHost = uri.Host.Replace(".blob.", ".dfs.");
            UriBuilder uriBuilder = new UriBuilder(uri.Scheme, newHost, uri.Port, uri.PathAndQuery);
            return uriBuilder.Uri;
        }

        /// <summary>
        /// Trims the trailing '/' from a Uri
        /// </summary>
        /// <param name="uri"><see cref="Uri"/> to trim</param>
        /// <returns></returns>
        public static Uri RemoveTrailingDelimiter(Uri uri)
        {
            if(uri == null)
            {
                return null;
            }

            if (uri.ToString().EndsWith("/", StringComparison.Ordinal))
            {
                uri = new Uri(uri.ToString().Substring(0, uri.ToString().Length - 1));
            }

            return uri;
        }
    }
}
