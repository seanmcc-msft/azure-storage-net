//-----------------------------------------------------------------------
// <copyright file="CloudBlobDirectory.cs" company="Microsoft">
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

namespace Microsoft.Azure.Storage.Blob
{
    using Microsoft.Azure.Storage.Blob.Protocol;
    using Microsoft.Azure.Storage.Core;
    using Microsoft.Azure.Storage.Core.Auth;
    using Microsoft.Azure.Storage.Core.Executor;
    using Microsoft.Azure.Storage.Core.Util;
    using Microsoft.Azure.Storage.Shared.Protocol;
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Security.AccessControl;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a virtual directory of blobs, designated by a delimiter character.
    /// </summary>
    /// <remarks>Containers, which are encapsulated as <see cref="CloudBlobContainer"/> objects, hold directories, and directories hold block blobs and page blobs. Directories can also contain sub-directories.</remarks>
    public partial class CloudBlobDirectory
    {
#if SYNC
        /// <summary>
        /// Returns an enumerable collection of the blobs in the virtual directory that are retrieved lazily.
        /// </summary>
        /// <param name="useFlatBlobListing">A boolean value that specifies whether to list blobs in a flat listing, or whether to list blobs hierarchically, by virtual directory.</param>
        /// <param name="blobListingDetails">A <see cref="BlobListingDetails"/> enumeration describing which items to include in the listing.</param>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request. If <c>null</c>, default options are applied to the request.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <returns>An enumerable collection of objects that implement <see cref="IListBlobItem"/> and are retrieved lazily.</returns>
        [DoesServiceRequest]
        public virtual IEnumerable<IListBlobItem> ListBlobs(bool useFlatBlobListing = false, BlobListingDetails blobListingDetails = BlobListingDetails.None, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            return this.Container.ListBlobs(this.Prefix, useFlatBlobListing, blobListingDetails, options, operationContext);
        }

        /// <summary>
        /// Returns a result segment containing a collection of blob items 
        /// in the virtual directory.
        /// </summary>
        /// <param name="currentToken">A <see cref="BlobContinuationToken"/> object returned by a previous listing operation.</param>
        /// <returns>A <see cref="BlobResultSegment"/> object.</returns>
        [DoesServiceRequest]
        public virtual BlobResultSegment ListBlobsSegmented(BlobContinuationToken currentToken)
        {
            return this.ListBlobsSegmented(false, BlobListingDetails.None, null /* maxResults */, currentToken, null /* options */, null /* operationContext */);
        }

        /// <summary>
        /// Returns a result segment containing a collection of blob items 
        /// in the virtual directory.
        /// </summary>
        /// <param name="useFlatBlobListing">A boolean value that specifies whether to list blobs in a flat listing, or whether to list blobs hierarchically, by virtual directory.</param>
        /// <param name="blobListingDetails">A <see cref="BlobListingDetails"/> enumeration describing which items to include in the listing.</param>
        /// <param name="maxResults">A non-negative integer value that indicates the maximum number of results to be returned at a time, up to the 
        /// per-operation limit of 5000. If this value is <c>null</c>, the maximum possible number of results will be returned, up to 5000.</param>    
        /// <param name="currentToken">A continuation token returned by a previous listing operation.</param> 
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="BlobResultSegment"/> object.</returns>
        [DoesServiceRequest]
        public virtual BlobResultSegment ListBlobsSegmented(bool useFlatBlobListing, BlobListingDetails blobListingDetails, int? maxResults, BlobContinuationToken currentToken, BlobRequestOptions options, OperationContext operationContext)
        {
            return this.Container.ListBlobsSegmented(this.Prefix, useFlatBlobListing, blobListingDetails, maxResults, currentToken, options, operationContext);
        }

        /// <summary>
        /// Creates a CloudBlobDirectory.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="umask">Optional and only valid if Hierarchical Namespace is enabled for the account.  When creating a directory and the parent folder
        /// does not have a default ACL, the umask restricts the permissions of the file or directory to be created.The resulting permission is given by p & ^u, 
        /// where p is the permission and u is the umask</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        [DoesServiceRequest]
        public virtual void Create(
            BlobRequestOptions options = null, 
            PathPermissions umask = null, 
            OperationContext operationContext = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            Executor.ExecuteSync(
                this.CreateImp(this.attributes, modifiedOptions, umask),
                modifiedOptions.RetryPolicy,
                operationContext);
        }

        /// <summary>
        /// Deletes a CloudBlobDirectory.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed. If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="continuation">Optional. When deleting a directory, the number of paths that are deleted with each invocation is limited. If the number of paths to be deleted exceeds this limit, a continuation token is returned in this response header. When a continuation token is returned in the response, it must be specified in a subsequent invocation of the delete operation to continue deleting the directory.</param>
        /// <returns>The continuation token to continue the delete operation, or null if it is complete.</returns>
        [DoesServiceRequest]
        public virtual string Delete(
            BlobRequestOptions options = null, 
            AccessCondition accessCondition = null, 
            OperationContext operationContext = null,
            string continuation = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            return Executor.ExecuteSync(
                this.DeleteImp(modifiedOptions, accessCondition, continuation),
                modifiedOptions.RetryPolicy,
                operationContext);
        }

        /// <summary>
        /// Moves a CloudBlobDirectory.
        /// </summary>
        /// <param name="destination">A <see cref="Uri"/> specifying the location to move CloudBlobDirectory to.</param>
        /// <param name="sourceAccessCondition">An <see cref="AccessCondition"/> object that represents the conditions on the source that must be met in 
        /// order for the request to proceed. If <c>null</c>, no condition is used.</param>
        /// <param name="destAccessCondition">An <see cref="AccessCondition"/> object that represents the conditions on the destination that must be met 
        /// in order for the request to proceed. If <c>null</c>, no condition is used.</param>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="umask">Optional and only valid if Hierarchical Namespace is enabled for the account. When creating a file or directory and the 
        /// parent folder does not have a default ACL, the umask restricts the permissions of the file or directory to be created. The resulting permission
        /// is given by p & ^u, where p is the permission and u is the umask.</param>
        /// <param name="mode">Optional. Valid only when namespace is enabled. This parameter determines the behavior of the rename operation.  Default value is "posix".</param>
        /// <param name="continuation">Optional. When renaming a directory, the number of paths that are renamed with each invocation is limited if hierarchical namespace is not
        /// enabled on the storage account.  If the number of paths to be renamed exceeds this limit, a continuation token is returned in this response header. When a continuation 
        /// token is returned in the response,
        /// it must be specified in a subsequent invocation of the rename operation to continue renaming the directory.</param>
        /// <returns>The continuation token to continue the delete operation.  Continuation token will be null or empty if the delete request is complete.</returns>
        [DoesServiceRequest]
        public virtual string Move(
            Uri destination,
            AccessCondition sourceAccessCondition = null,
            AccessCondition destAccessCondition = null,
            BlobRequestOptions options = null,
            OperationContext operationContext = null,
            PathPermissions umask = null,
            PathRenameMode? mode = null,
            string continuation = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            return Executor.ExecuteSync(
                this.MoveImp(destination, sourceAccessCondition, destAccessCondition, modifiedOptions, umask, mode, continuation),
                modifiedOptions.RetryPolicy,
                operationContext);
        }

        /// <summary>
        /// Fetches the access controls for this CloudBlobDirectory.   Storage account must have hierarchical namespace enabled.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed.
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="upn">Optional. Valid only when Hierarchical Namespace is enabled for the account. If "true", the user identity values returned in the owner, 
        /// group, and acl will be transformed from Azure Active Directory Object IDs to User Principal Names. If "false", the values will be returned as Azure Active Directory Object IDs.</param>
        [DoesServiceRequest]
        public virtual void FetchAccessControls(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null,
            bool? upn = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            Executor.ExecuteSync(
                this.FetchAccessControlsImp(modifiedOptions, accessCondition, upn),
                modifiedOptions.RetryPolicy,
                operationContext);
        }

        /// <summary>
        /// Sets the permissions for this CloudBlobDirectory.  Storage account must have hierarchical namespace enabled.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed. 
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        [DoesServiceRequest]
        public virtual void SetPermissions(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            Executor.ExecuteSync(
                this.SetPermissionsImp(this.attributes, modifiedOptions, accessCondition),
                modifiedOptions.RetryPolicy,
                operationContext);
        }

        /// <summary>
        /// Sets the ACL for this CloudBlobDirectory.  Storage account must have hierarchical namespace enabled.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed. 
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        [DoesServiceRequest]
        public virtual void SetAcl(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            Executor.ExecuteSync(
                this.SetAclImp(this.attributes, modifiedOptions, accessCondition),
                modifiedOptions.RetryPolicy,
                operationContext);
        }

        /// <summary>
        /// Fetches the metadata for this CloudBlobDirectory.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed.
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        [DoesServiceRequest]
        public virtual void FetchAttributes(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            Executor.ExecuteSync(
                this.FetchAttributesImpl(this.attributes, accessCondition, modifiedOptions),
                modifiedOptions.RetryPolicy,
                operationContext);
        }

        /// <summary>
        /// Determinds if a CloudBlobDirectory exists.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed.
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        [DoesServiceRequest]
        public virtual bool Exists(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            return Executor.ExecuteSync(
                this.ExistsImpl(this.attributes, accessCondition, modifiedOptions),
                modifiedOptions.RetryPolicy,
                operationContext);
        }

        /// <summary>
        /// Sets the metadata for this CloudBlobDirectory.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed.
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        [DoesServiceRequest]
        public virtual void SetMetadata(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            Executor.ExecuteSync(
                this.SetMetadataImp(this.attributes, modifiedOptions, accessCondition),
                modifiedOptions.RetryPolicy,
                operationContext);
        }

        /// <summary>
        /// Sets the properties for this CloudBlobDirectory.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed.
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        [DoesServiceRequest]
        public virtual void SetProperties(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            Executor.ExecuteSync(
                this.SetPropertiesImp(this.attributes, accessCondition, modifiedOptions),
                modifiedOptions.RetryPolicy,
                operationContext);
        }

#endif

        /// <summary>
        /// Begins an asynchronous operation to return a result segment containing a collection of blob items 
        /// in the virtual directory.
        /// </summary>
        /// <param name="currentToken">A continuation token returned by a previous listing operation.</param>
        /// <param name="callback">An <see cref="AsyncCallback"/> delegate that will receive notification when the asynchronous operation completes.</param>
        /// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        /// <returns>An <see cref="ICancellableAsyncResult"/> that references the asynchronous operation.</returns>
        [DoesServiceRequest]
        public virtual ICancellableAsyncResult BeginListBlobsSegmented(BlobContinuationToken currentToken, AsyncCallback callback, object state)
        {
            return this.BeginListBlobsSegmented(false, BlobListingDetails.None, null /* maxResults */, currentToken, null /* options */, null /* operationContext */, callback, state);
        }

        /// <summary>
        /// Begins an asynchronous operation to return a result segment containing a collection of blob items 
        /// in the virtual directory.
        /// </summary>
        /// <param name="useFlatBlobListing">A boolean value that specifies whether to list blobs in a flat listing, or whether to list blobs hierarchically, by virtual directory.</param>
        /// <param name="blobListingDetails">A <see cref="BlobListingDetails"/> enumeration describing which items to include in the listing.</param>
        /// <param name="maxResults">A non-negative integer value that indicates the maximum number of results to be returned at a time, up to the 
        /// per-operation limit of 5000. If this value is <c>null</c>, the maximum possible number of results will be returned, up to 5000.</param>  
        /// <param name="currentToken">A continuation token returned by a previous listing operation.</param> 
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="callback">An <see cref="AsyncCallback"/> delegate that will receive notification when the asynchronous operation completes.</param>
        /// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        /// <returns>An <see cref="ICancellableAsyncResult"/> that references the asynchronous operation.</returns>
        [DoesServiceRequest]
        public virtual ICancellableAsyncResult BeginListBlobsSegmented(bool useFlatBlobListing, BlobListingDetails blobListingDetails, int? maxResults, BlobContinuationToken currentToken, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return CancellableAsyncResultTaskWrapper.Create(token => this.ListBlobsSegmentedAsync(useFlatBlobListing, blobListingDetails, maxResults, currentToken, options, operationContext, token), callback, state);
        }

        /// <summary>
        /// Ends an asynchronous operation to return a result segment containing a collection of blob items 
        /// in the virtual directory.
        /// </summary>
        /// <param name="asyncResult">An <see cref="IAsyncResult"/> that references the pending asynchronous operation.</param>
        /// <returns>A <see cref="BlobResultSegment"/> object.</returns>
        public virtual BlobResultSegment EndListBlobsSegmented(IAsyncResult asyncResult)
        {
            return ((CancellableAsyncResultTaskWrapper<BlobResultSegment>)asyncResult).GetAwaiter().GetResult();
        }
        
#if TASK
        /// <summary>
        /// Initiates an asynchronous operation to return a result segment containing a collection of blob items 
        /// in the virtual directory.
        /// </summary>
        /// <param name="currentToken">A continuation token returned by a previous listing operation.</param> 
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="BlobResultSegment"/>.</returns>
        [DoesServiceRequest]
        public virtual Task<BlobResultSegment> ListBlobsSegmentedAsync(BlobContinuationToken currentToken)
        {
            return this.Container.ListBlobsSegmentedAsync(this.Prefix, false /*useFlatBlobListDetails*/, BlobListingDetails.None, null /*maxResults*/, currentToken, default(BlobRequestOptions), default(OperationContext), CancellationToken.None);
        }

        /// <summary>
        /// Initiates an asynchronous operation to return a result segment containing a collection of blob items 
        /// in the virtual directory.
        /// </summary>
        /// <param name="currentToken">A continuation token returned by a previous listing operation.</param> 
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="BlobResultSegment"/>.</returns>
        [DoesServiceRequest]
        public virtual Task<BlobResultSegment> ListBlobsSegmentedAsync(BlobContinuationToken currentToken, CancellationToken cancellationToken)
        {
            return this.Container.ListBlobsSegmentedAsync(this.Prefix, false /*useFlatBlobListDetails*/, BlobListingDetails.None, null /*maxResults*/, currentToken, default(BlobRequestOptions), default(OperationContext), cancellationToken);
        }
        
        /// <summary>
        /// Initiates an asynchronous operation to return a result segment containing a collection of blob items 
        /// in the virtual directory.
        /// </summary>
        /// <param name="useFlatBlobListing">A boolean value that specifies whether to list blobs in a flat listing, or whether to list blobs hierarchically, by virtual directory.</param>
        /// <param name="blobListingDetails">A <see cref="BlobListingDetails"/> enumeration describing which items to include in the listing.</param>
        /// <param name="maxResults">A non-negative integer value that indicates the maximum number of results to be returned at a time, up to the 
        /// per-operation limit of 5000. If this value is <c>null</c>, the maximum possible number of results will be returned, up to 5000.</param>  
        /// <param name="currentToken">A continuation token returned by a previous listing operation.</param> 
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="BlobResultSegment"/>.</returns>
        [DoesServiceRequest]
        public virtual Task<BlobResultSegment> ListBlobsSegmentedAsync(bool useFlatBlobListing, BlobListingDetails blobListingDetails, int? maxResults, BlobContinuationToken currentToken, BlobRequestOptions options, OperationContext operationContext)
        {
            return this.ListBlobsSegmentedAsync(useFlatBlobListing, blobListingDetails, maxResults, currentToken, options, operationContext, CancellationToken.None);
        }
        
        /// <summary>
        /// Initiates an asynchronous operation to return a result segment containing a collection of blob items 
        /// in the virtual directory.
        /// </summary>
        /// <param name="useFlatBlobListing">A boolean value that specifies whether to list blobs in a flat listing, or whether to list blobs hierarchically, by virtual directory.</param>
        /// <param name="blobListingDetails">A <see cref="BlobListingDetails"/> enumeration describing which items to include in the listing.</param>
        /// <param name="maxResults">A non-negative integer value that indicates the maximum number of results to be returned at a time, up to the 
        /// per-operation limit of 5000. If this value is <c>null</c>, the maximum possible number of results will be returned, up to 5000.</param>  
        /// <param name="currentToken">A continuation token returned by a previous listing operation.</param> 
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for a task to complete.</param>
        /// <returns>A <see cref="Task{T}"/> object of type <see cref="BlobResultSegment"/>.</returns>
        [DoesServiceRequest]
        public virtual Task<BlobResultSegment> ListBlobsSegmentedAsync(bool useFlatBlobListing, BlobListingDetails blobListingDetails, int? maxResults, BlobContinuationToken currentToken, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return this.Container.ListBlobsSegmentedAsync(this.Prefix, useFlatBlobListing, blobListingDetails, maxResults, currentToken, options, operationContext, cancellationToken);
        }

        /// <summary>
        /// Creates a CloudBlobDirectory.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="umask">Optional and only valid if Hierarchical Namespace is enabled for the account.  When creating a directory and the parent folder does not have a default ACL, 
        /// the umask restricts the permissions of the file or directory to be created.The resulting permission is given by p & ^u, where p is the permission and u is the umask</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for a task to complete.</param>
        [DoesServiceRequest]
        public virtual Task CreateAsync(
            BlobRequestOptions options = null,
            PathPermissions umask = null,
            OperationContext operationContext = null,
            CancellationToken? cancellationToken = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            return Executor.ExecuteAsync(
                this.CreateImp(this.attributes, modifiedOptions, umask),
                modifiedOptions.RetryPolicy,
                operationContext,
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes a CloudBlobDirectory.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed.
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="continuation">Optional. When deleting a directory, the number of paths that are deleted with each invocation is limited if hierarchical namespace 
        /// is not enabled for the storage account.  If the number of paths to be deleted exceeds this limit, a continuation token is returned in this response header. 
        /// When a continuation token is returned in the response, it must be specified in a subsequent invocation of the delete operation to continue deleting the directory.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for a task to complete.</param>
        /// <returns>The continuation token to continue the delete operation, or null if it is complete.</returns>
        [DoesServiceRequest]
        public virtual Task<string> DeleteAsync(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null,
            string continuation = null,
            CancellationToken? cancellationToken = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            return Executor.ExecuteAsync(
                this.DeleteImp(modifiedOptions, accessCondition, continuation),
                modifiedOptions.RetryPolicy,
                operationContext,
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Moves a CloudBlobDirectory.
        /// </summary>
        /// <param name="destination">A <see cref="Uri"/> specifying the location to move CloudBlobDirectory to.</param>
        /// <param name="sourceAccessCondition">An <see cref="AccessCondition"/> object that represents the conditions on the source that must be met in order for the request to proceed. 
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="destAccessCondition">An <see cref="AccessCondition"/> object that represents the conditions on the destination that must be met in order for the request to proceed. 
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="umask">Optional and only valid if Hierarchical Namespace is enabled for the account. When creating a file or directory and the parent folder does not 
        /// have a default ACL, the umask restricts the permissions of the file or directory to be created. The resulting permission is given by p & ^u, where p is the permission and u is the umask.</param>
        /// <param name="mode">Optional. Valid only when namespace is enabled. This parameter determines the behavior of the rename operation.  Default value is "posix".</param>
        /// <param name="continuation">Optional. When renaming a directory, the number of paths that are renamed with each invocation is limited if hierarchical namespace is not enabled on the storge account. 
        /// If the number of paths to be renamed exceeds this limit, a continuation token is returned in this response header. When a continuation token is returned in the response, it must be specified in a 
        /// subsequent invocation of the rename operation to continue renaming the directory.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for a task to complete.</param>
        /// <returns>The continuation token to continue the delete operation.  Continuation token will be null or empty if the delete request is complete.</returns>
        [DoesServiceRequest]
        public virtual Task<string> MoveAsync(
            Uri destination,
            AccessCondition sourceAccessCondition = null,
            AccessCondition destAccessCondition = null,
            BlobRequestOptions options = null,
            OperationContext operationContext = null,
            PathPermissions umask = null,
            PathRenameMode? mode = null,
            string continuation = null,
            CancellationToken? cancellationToken = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            return Executor.ExecuteAsync(
                this.MoveImp(destination, sourceAccessCondition, destAccessCondition, modifiedOptions, umask, mode, continuation),
                modifiedOptions.RetryPolicy,
                operationContext,
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Fetches the access controls for this CloudBlobDirectory.   Storage account must have hierarchical namespace enabled.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed. 
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="upn">Optional. Valid only when Hierarchical Namespace is enabled for the account. If "true", the user identity values returned in the owner, 
        /// group, and acl will be transformed from Azure Active Directory Object IDs to User Principal Names. If "false", the values will be returned as Azure Active Directory Object IDs. </param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for a task to complete.</param>
        [DoesServiceRequest]
        public virtual Task FetchAccessControlsAsync(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null,
            bool? upn = null,
            CancellationToken? cancellationToken = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            return Executor.ExecuteAsync(
                this.FetchAccessControlsImp(modifiedOptions, accessCondition, upn),
                modifiedOptions.RetryPolicy,
                operationContext,
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Sets the permissions for this CloudBlobDirectory.   Storage account must have hierarchical namespace enabled.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed. 
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for a task to complete.</param>
        [DoesServiceRequest]
        public virtual Task SetPermissionsAsync(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null,
            CancellationToken? cancellationToken = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            return Executor.ExecuteAsync(
                this.SetPermissionsImp(this.attributes, modifiedOptions, accessCondition),
                modifiedOptions.RetryPolicy,
                operationContext,
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Sets the ACL for this CloudBlobDirectory.  Storage account must have hierarchical namespace enabled.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed. 
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for a task to complete.</param>
        [DoesServiceRequest]
        public virtual Task SetAclAsync(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null,
            CancellationToken? cancellationToken = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            return Executor.ExecuteAsync(
                this.SetAclImp(this.attributes, modifiedOptions, accessCondition),
                modifiedOptions.RetryPolicy,
                operationContext,
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Fetches the metadata for this CloudBlobDirectory.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed. 
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for a task to complete.</param>
        [DoesServiceRequest]
        public virtual Task FetchAttributesAsync(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null,
            CancellationToken? cancellationToken = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            return Executor.ExecuteAsync(
                this.FetchAttributesImpl(this.attributes, accessCondition, modifiedOptions),
                modifiedOptions.RetryPolicy,
                operationContext,
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Determinds if a CloudBlobDirectory exists.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed.
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for a task to complete.</param>
        [DoesServiceRequest]
        public virtual Task<bool> ExistsAsync(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null,
            CancellationToken? cancellationToken = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            return Executor.ExecuteAsync(
                this.ExistsImpl(this.attributes, accessCondition, modifiedOptions),
                modifiedOptions.RetryPolicy,
                operationContext,
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Sets the metadata for this CloudBlobDirectory.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed.
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for a task to complete.</param>
        [DoesServiceRequest]
        public virtual Task SetMetadataAsync(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null,
            CancellationToken? cancellationToken = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            return Executor.ExecuteAsync(
                this.SetMetadataImp(this.attributes, modifiedOptions, accessCondition),
                modifiedOptions.RetryPolicy,
                operationContext,
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Sets the properties for this CloudBlobDirectory.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the condition that must be met in order for the request to proceed. 
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object that represents the context for the current operation.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for a task to complete.</param>
        [DoesServiceRequest]
        public virtual Task SetPropertiesAsync(
            BlobRequestOptions options = null,
            AccessCondition accessCondition = null,
            OperationContext operationContext = null,
            CancellationToken? cancellationToken = null)
        {
            BlobRequestOptions modifiedOptions = BlobRequestOptions.ApplyDefaults(options, BlobType.Unspecified, this.ServiceClient);
            return Executor.ExecuteAsync(
                this.SetPropertiesImp(this.attributes, accessCondition, modifiedOptions),
                modifiedOptions.RetryPolicy,
                operationContext,
                cancellationToken ?? CancellationToken.None);
        }
#endif    
        /// <summary>
        ///  Implementation method for the Create method.
        /// </summary>
        /// <param name="blobAttributes">The attributes.</param>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="umask">An optional <see cref="PathPermissions"/> object to specify umask</param>
        internal RESTCommand<NullType> CreateImp(
            BlobAttributes blobAttributes,
            BlobRequestOptions options, 
            PathPermissions umask)
        {
            RESTCommand<NullType> putCmd = new RESTCommand<NullType>(this.ServiceClient.Credentials, this.StorageUri, this.ServiceClient.HttpClient);

            options.ApplyToStorageCommand(putCmd);
            putCmd.BuildRequest = (cmd, uri, builder, cnt, serverTimeout, ctx) =>
            {
                StorageRequestMessage msg = BlobHttpRequestMessageFactory.CreatePath(
                    uri: uri, 
                    sourceUri: null, 
                    timeout: serverTimeout, 
                    properties: this.Properties, 
                    pathProperties: this.PathProperties, 
                    metadata: this.Metadata,
                    sourceAccessCondition: null, 
                    destAccessCondition: null, 
                    content: null, 
                    operationContext: ctx, 
                    canonicalizer: this.ServiceClient.GetCanonicalizer(), 
                    credentials: this.ServiceClient.Credentials, 
                    resourceType: Constants.DirectoryResource,
                    mode: null, 
                    umask: umask,
                    continuation: null,
                    move: false);
                return msg;
            };
            putCmd.PreProcessResponse = (cmd, resp, ex, ctx) =>
            {
                HttpResponseParsers.ProcessExpectedStatusCodeNoException(HttpStatusCode.Created, resp, NullType.Value, cmd, ex);
                CloudBlob.UpdateETagLMTLengthAndSequenceNumber(blobAttributes, resp, false);
                return NullType.Value;
            };

            return putCmd;
        }

        /// <summary>
        /// Implementation method for the Move method.
        /// </summary>
        /// <param name="destination">Location to move the CloubBlockBlob to</param>
        /// <param name="sourceAccessCondition">An <see cref="AccessCondition"/> object that represents the conditions on the source that must be met in order for the request to proceed.
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="destAccessCondition">An <see cref="AccessCondition"/> object that represents the conditions on the destination that must be met in order for the request to proceed. 
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="umask">An optional <see cref="PathPermissions"/> object to specify umask</param>
        /// <param name="mode">An optional <see cref="PathRenameMode"/> object to specify rename mode</param>
        /// <param name="continuation">Continuation token for large move requests</param>
        /// <returns></returns>
        internal RESTCommand<string> MoveImp(
            Uri destination, 
            AccessCondition sourceAccessCondition,
            AccessCondition destAccessCondition,
            BlobRequestOptions options, 
            PathPermissions umask,
            PathRenameMode? mode,
            string continuation)
        {
            RESTCommand<string> putCmd = new RESTCommand<string>(this.ServiceClient.Credentials, this.StorageUri, this.ServiceClient.HttpClient);

            options.ApplyToStorageCommand(putCmd);
            putCmd.BuildRequest = (cmd, uri, builder, cnt, serverTimeout, ctx) =>
            {
                StorageRequestMessage msg = BlobHttpRequestMessageFactory.CreatePath(
                    uri: destination, 
                    sourceUri: this.StorageUri.PrimaryUri, 
                    timeout: serverTimeout, 
                    properties: this.Properties, 
                    pathProperties: this.PathProperties, 
                    metadata: this.Metadata, 
                    sourceAccessCondition: sourceAccessCondition, 
                    destAccessCondition: destAccessCondition, 
                    content: cnt, 
                    operationContext: ctx, 
                    canonicalizer: this.ServiceClient.GetCanonicalizer(), 
                    credentials: this.ServiceClient.Credentials, 
                    resourceType: Constants.DirectoryResource, 
                    mode: mode, 
                    umask: umask, 
                    continuation: continuation,
                    move: true);
                return msg;
            };
            putCmd.PreProcessResponse = (cmd, resp, ex, ctx) =>
            {
                return HttpResponseParsers.ProcessExpectedStatusCodeNoException(HttpStatusCode.Created, resp, null, cmd, ex);
            };
            putCmd.PostProcessResponseAsync = async (cmd, resp, ctx, ct) =>
            {
                return HttpResponseParsers.GetHeader(resp, Constants.HeaderConstants.Continuation);
            };

            return putCmd;
        }

        /// <summary>
        /// Implementation method for the Delete method.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the access conditions for the blob.
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="continuation">Optional. When deleting a directory, the number of paths that are deleted with each invocation is 
        /// limited if hierarchical namespace is not enabled on the storage account.. If the number of paths to be deleted exceeds this limit, a 
        /// continuation token is returned in this response header. When a continuation token is returned in the response, it must be specified in a 
        /// subsequent invocation of the delete operation to continue deleting the directory.</param>
        /// <returns>The continuation token for the next delete request or null</returns>
        internal RESTCommand<string> DeleteImp(
            BlobRequestOptions options, 
            AccessCondition accessCondition,
            string continuation)
        {
            RESTCommand<string> deleteCmd = new RESTCommand<string>(this.ServiceClient.Credentials, this.StorageUri, this.ServiceClient.HttpClient);

            options.ApplyToStorageCommand(deleteCmd);
            deleteCmd.BuildRequest = (cmd, uri, builder, cnt, serverTimeout, ctx) =>
            {
                StorageRequestMessage msg = BlobHttpRequestMessageFactory.DeletePath(
                    uri: uri, 
                    timeout: serverTimeout, 
                    accessCondition: accessCondition, 
                    operationContext: ctx, 
                    canonicalizer: this.ServiceClient.GetCanonicalizer(),
                    credentials: this.ServiceClient.Credentials, 
                    recursive: true, 
                    continuation: continuation);
                return msg;
            };
            deleteCmd.PreProcessResponse = (cmd, resp, ex, ctx) =>
            {
                return HttpResponseParsers.ProcessExpectedStatusCodeNoException(HttpStatusCode.OK, resp, null, cmd, ex);
            };
            deleteCmd.PostProcessResponseAsync = async (cmd, resp, ctx, ct) =>
            {
                return HttpResponseParsers.GetHeader(resp, Constants.HeaderConstants.Continuation);
            };

            return deleteCmd;
        }

        /// <summary>
        /// Implementation method for the FetchAccessControls method.
        /// </summary>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the access conditions for the blob. 
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="upn">Optional. Valid only when Hierarchical Namespace is enabled for the account. If "true", the user identity 
        /// values returned in the owner, group, and acl will be transformed from Azure Active Directory Object IDs to User Principal Names. 
        /// If "false", the values will be returned as Azure Active Directory Object IDs.</param>
        /// <returns></returns>
        internal RESTCommand<NullType> FetchAccessControlsImp(
            BlobRequestOptions options,
            AccessCondition accessCondition,
            bool? upn)
        {
            RESTCommand<NullType> headCmd = new RESTCommand<NullType>(this.ServiceClient.Credentials, this.StorageUri, this.ServiceClient.HttpClient);

            options.ApplyToStorageCommand(headCmd);
            headCmd.BuildRequest = (cmd, uri, builder, cnt, serverTimeout, ctx) =>
            {
                StorageRequestMessage msg = BlobHttpRequestMessageFactory.GetPathProperties(
                    uri: uri,
                    timeout: serverTimeout,
                    accessCondition: accessCondition,
                    operationContext: ctx,
                    canonicalizer: this.ServiceClient.GetCanonicalizer(),
                    credentials: this.ServiceClient.Credentials,
                    action: Constants.GetAccessControlAction,
                    upn: upn);
                return msg;
            };
            headCmd.PreProcessResponse = (cmd, resp, ex, ctx) =>
            {
                HttpResponseParsers.ProcessExpectedStatusCodeNoException(HttpStatusCode.OK, resp, NullType.Value, cmd, ex);
                this.PathProperties = BlobHttpResponseParsers.ParseBlobAccessControls(resp);
                return NullType.Value;
            };

            return headCmd;
        }

        /// <summary>
        /// Implementation method for the SetPermissions method.
        /// </summary>
        /// <param name="blobAttributes">The attributes.</param>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the access conditions for the blob. 
        /// If <c>null</c>, no condition is used.</param>
        /// <returns></returns>
        internal RESTCommand<NullType> SetPermissionsImp(
            BlobAttributes blobAttributes,
            BlobRequestOptions options,
            AccessCondition accessCondition)
        {
            RESTCommand<NullType> patchCmd = new RESTCommand<NullType>(this.ServiceClient.Credentials, this.StorageUri, this.ServiceClient.HttpClient);
            options.ApplyToStorageCommand(patchCmd);
            patchCmd.BuildRequest = (cmd, uri, builder, cnt, serverTimeout, ctx) =>
            {
                StorageRequestMessage msg = BlobHttpRequestMessageFactory.SetPermissions(
                    uri: uri,
                    timeout: serverTimeout,
                    pathAccessControls: this.PathProperties,
                    accessCondition: accessCondition,
                    operationContext: ctx,
                    canonicalizer: this.ServiceClient.GetCanonicalizer(),
                    credentials: this.ServiceClient.Credentials);
                return msg;
            };
            patchCmd.PreProcessResponse = (cmd, resp, ex, ctx) =>
            {
                HttpResponseParsers.ProcessExpectedStatusCodeNoException(HttpStatusCode.OK, resp, NullType.Value, cmd, ex);
                CloudBlob.UpdateETagLMTLengthAndSequenceNumber(blobAttributes, resp, false);
                return NullType.Value;
            };

            return patchCmd;
        }

        /// <summary>
        /// Implementation method for the SetAcl method.
        /// </summary>
        /// <param name="blobAttributes">The attributes.</param>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the access conditions for the blob.
        /// If <c>null</c>, no condition is used.</param>
        /// <returns></returns>
        internal RESTCommand<NullType> SetAclImp(
            BlobAttributes blobAttributes,
            BlobRequestOptions options,
            AccessCondition accessCondition)
        {
            RESTCommand<NullType> patchCmd = new RESTCommand<NullType>(this.ServiceClient.Credentials, this.StorageUri, this.ServiceClient.HttpClient);
            options.ApplyToStorageCommand(patchCmd);
            patchCmd.BuildRequest = (cmd, uri, builder, cnt, serverTimeout, ctx) =>
            {
                StorageRequestMessage msg = BlobHttpRequestMessageFactory.SetACL(
                    uri: uri,
                    timeout: serverTimeout,
                    acl: this.PathProperties.ACL,
                    accessCondition: accessCondition,
                    operationContext: ctx,
                    canonicalizer: this.ServiceClient.GetCanonicalizer(),
                    credentials: this.ServiceClient.Credentials);
                return msg;
            };
            patchCmd.PreProcessResponse = (cmd, resp, ex, ctx) =>
            {
                HttpResponseParsers.ProcessExpectedStatusCodeNoException(HttpStatusCode.OK, resp, NullType.Value, cmd, ex);
                CloudBlob.UpdateETagLMTLengthAndSequenceNumber(blobAttributes, resp, false);
                return NullType.Value;
            };

            return patchCmd;
        }

        /// <summary>
        ///  Implementation method for the SetProperties method.
        /// </summary>
        /// <param name="blobAttributes">The attributes.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the access conditions for the blob. 
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        internal RESTCommand<NullType> SetPropertiesImp(
            BlobAttributes blobAttributes,
            AccessCondition accessCondition,
            BlobRequestOptions options)
        {
            RESTCommand<NullType> patchCmd = new RESTCommand<NullType>(this.ServiceClient.Credentials, this.StorageUri, this.ServiceClient.HttpClient);

            options.ApplyToStorageCommand(patchCmd);
            patchCmd.BuildRequest = (cmd, uri, builder, cnt, serverTimeout, ctx) =>
            {
                StorageRequestMessage msg = BlobHttpRequestMessageFactory.SetProperties(
                    uri: uri,
                    timeout: serverTimeout,
                    properties: this.Properties,
                    accessCondition: accessCondition,
                    content: null,
                    operationContext: ctx,
                    canonicalizer: this.ServiceClient.GetCanonicalizer(),
                    credentials: this.ServiceClient.Credentials);
                BlobHttpRequestMessageFactory.AddMetadata(msg, blobAttributes.Metadata);
                return msg;
            };
            patchCmd.PreProcessResponse = (cmd, resp, ex, ctx) =>
            {
                HttpResponseParsers.ProcessExpectedStatusCodeNoException(HttpStatusCode.OK, resp, NullType.Value, cmd, ex);
                CloudBlob.UpdateETagLMTLengthAndSequenceNumber(blobAttributes, resp, false);
                return NullType.Value;
            };

            return patchCmd;
        }

        /// <summary>
        ///  Implementation method for the FetchMetadata method.
        /// </summary>
        /// <param name="blobAttributes">The attributes.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the access conditions for the blob.
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        internal RESTCommand<NullType> FetchAttributesImpl(
            BlobAttributes blobAttributes,
            AccessCondition accessCondition,
            BlobRequestOptions options)
        {
            RESTCommand<NullType> headCmd = new RESTCommand<NullType>(this.ServiceClient.Credentials, this.StorageUri, this.ServiceClient.HttpClient);

            options.ApplyToStorageCommand(headCmd);
            headCmd.BuildRequest = (cmd, uri, builder, cnt, serverTimeout, ctx) =>
            {
                StorageRequestMessage msg = BlobHttpRequestMessageFactory.GetProperties(
                    uri: uri,
                    timeout: serverTimeout,
                    snapshot: null,
                    accessCondition: accessCondition,
                    content: null,
                    operationContext: ctx,
                    canonicalizer: this.ServiceClient.GetCanonicalizer(),
                    credentials: this.ServiceClient.Credentials);
                return msg;
            };
            headCmd.PreProcessResponse = (cmd, resp, ex, ctx) =>
            {
                HttpResponseParsers.ProcessExpectedStatusCodeNoException(HttpStatusCode.OK, resp, NullType.Value, cmd, ex);
                CloudBlob.UpdateAfterFetchAttributes(blobAttributes, resp);
                return NullType.Value;
            };

            return headCmd;
        }

        /// <summary>
        ///  Implementation method for the Exists method.
        /// </summary>
        /// <param name="blobAttributes">The attributes.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the access conditions for the blob.
        /// If <c>null</c>, no condition is used.</param>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        internal RESTCommand<bool> ExistsImpl(
            BlobAttributes blobAttributes,
            AccessCondition accessCondition,
            BlobRequestOptions options)
        {
            RESTCommand<bool> headCmd = new RESTCommand<bool>(this.ServiceClient.Credentials, this.StorageUri, this.ServiceClient.HttpClient);

            options.ApplyToStorageCommand(headCmd);
            headCmd.BuildRequest = (cmd, uri, builder, cnt, serverTimeout, ctx) =>
            {
                StorageRequestMessage msg = BlobHttpRequestMessageFactory.GetProperties(
                    uri: uri,
                    timeout: serverTimeout,
                    snapshot: null,
                    accessCondition: accessCondition,
                    content: null,
                    operationContext: ctx,
                    canonicalizer: this.ServiceClient.GetCanonicalizer(),
                    credentials: this.ServiceClient.Credentials);
                return msg;
            };
            headCmd.PreProcessResponse = (cmd, resp, ex, ctx) =>
            {
                if (resp.StatusCode == HttpStatusCode.NotFound)
                {
                    return false;
                }

                HttpResponseParsers.ProcessExpectedStatusCodeNoException(HttpStatusCode.OK, resp, true, cmd, ex);
                CloudBlob.UpdateAfterFetchAttributes(blobAttributes, resp);
                return true;
            };

            return headCmd;
        }

        /// <summary>
        /// Implementation method for the SetMetadata method.
        /// </summary>
        /// <param name="blobAttributes">The attributes.</param>
        /// <param name="options">A <see cref="BlobRequestOptions"/> object that specifies additional options for the request.</param>
        /// <param name="accessCondition">An <see cref="AccessCondition"/> object that represents the access conditions for the blob. 
        /// If <c>null</c>, no condition is used.</param>
        internal RESTCommand<NullType> SetMetadataImp(
            BlobAttributes blobAttributes,
            BlobRequestOptions options,
            AccessCondition accessCondition)
        {
            RESTCommand<NullType> putCmd = new RESTCommand<NullType>(this.ServiceClient.Credentials, this.StorageUri, this.ServiceClient.HttpClient);

            options.ApplyToStorageCommand(putCmd);
            putCmd.BuildRequest = (cmd, uri, builder, cnt, serverTimeout, ctx) =>
            {
                StorageRequestMessage msg = BlobHttpRequestMessageFactory.SetMetadata(
                    uri: uri,
                    timeout: serverTimeout,
                    accessCondition: accessCondition,
                    content: null,
                    operationContext: ctx,
                    canonicalizer: this.ServiceClient.GetCanonicalizer(),
                    credentials: this.ServiceClient.Credentials);
                this.Metadata.Remove(Constants.Hdi_IsFolder);
                BlobHttpRequestMessageFactory.AddMetadata(msg, this.Metadata);
                return msg;
            };
            putCmd.PreProcessResponse = (cmd, resp, ex, ctx) =>
            {
                HttpResponseParsers.ProcessExpectedStatusCodeNoException(HttpStatusCode.OK, resp, NullType.Value, cmd, ex);
                CloudBlob.UpdateETagLMTLengthAndSequenceNumber(blobAttributes, resp, false);
                cmd.CurrentResult.IsRequestServerEncrypted = HttpResponseParsers.ParseServerRequestEncrypted(resp);
                return NullType.Value;
            };

            return putCmd;
        }
    }
}
