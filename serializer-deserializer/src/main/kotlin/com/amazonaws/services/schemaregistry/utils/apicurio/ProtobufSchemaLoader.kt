/*
 * Copyright 2021 Red Hat
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.utils.apicurio

import com.google.common.base.Charsets
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Lists
import com.google.common.io.CharStreams
import com.squareup.wire.schema.Location
import com.squareup.wire.schema.ProtoFile
import com.squareup.wire.schema.Schema
import com.squareup.wire.schema.SchemaLoader
import okio.Buffer
import okio.Path
import okio.Path.Companion.toPath
import okio.fakefilesystem.FakeFileSystem
import java.io.IOException
import java.io.InputStreamReader
import java.util.Collections
import java.util.Optional

class ProtobufSchemaLoader {
    class ProtobufSchemaLoaderContext internal constructor(
        private val schema: Schema,
        private val protoFile: ProtoFile,
    ) {
        fun getSchema(): Schema = schema

        fun getProtoFile(): ProtoFile = protoFile
    }

    companion object {
        private const val GOOGLE_API_PATH = "google/type/"
        private const val GOOGLE_WELLKNOWN_PATH = "google/protobuf/"
        private const val METADATA_PATH = "metadata/"
        private const val DECIMAL_PATH = "additionalTypes/"

        // Adding pre-built support for commonly used Google API Protos,
        // https://github.com/googleapis/googleapis
        // These files need to be manually loaded into the FileSystem
        // as Square doesn't support them by default.
        private val GOOGLE_API_PROTOS: Set<String> =
            ImmutableSet
                .builder<String>()
                .add("money.proto")
                .add("timeofday.proto")
                .add("date.proto")
                .add("calendar_period.proto")
                .add("color.proto")
                .add("dayofweek.proto")
                .add("latlng.proto")
                .add("fraction.proto")
                .add("month.proto")
                .add("phone_number.proto")
                .add("postal_address.proto")
                .add("localized_text.proto")
                .add("interval.proto")
                .add("expr.proto")
                .add("quaternion.proto")
                .build()

        // Adding support for Protobuf well-known types under package google.protobuf that are not covered by Square
        // https://developers.google.com/protocol-buffers/docs/reference/google.protobuf
        // These files need to be manually loaded into the FileSystem
        // as Square doesn't support them by default.
        private val GOOGLE_WELLKNOWN_PROTOS: Set<String> =
            ImmutableSet
                .builder<String>()
                .add("any.proto")
                .add("api.proto")
                .add("descriptor.proto")
                .add("duration.proto")
                .add("empty.proto")
                .add("field_mask.proto")
                .add("source_context.proto")
                .add("struct.proto")
                .add("timestamp.proto")
                .add("type.proto")
                .add("wrappers.proto")
                .build()

        // Adding support for Wire library protobuf extensions
        private const val WIRE_PATH = "wire/"
        private val WIRE_PROTOS: Set<String> =
            ImmutableSet
                .builder<String>()
                .add("extensions.proto")
                .build()

        private const val METADATA_PROTO = "metadata.proto"
        private const val DECIMAL_PROTO = "decimal.proto"

        @Throws(IOException::class)
        private fun getFileSystem(): FakeFileSystem {
            val inMemoryFileSystem = FakeFileSystem()
            inMemoryFileSystem.workingDirectory = "/".toPath()
            inMemoryFileSystem.allowSymlinks = true

            val classLoader = ProtobufSchemaLoader::class.java.classLoader

            createDirectory(GOOGLE_API_PATH.split("/").dropLastWhile { it.isEmpty() }.toTypedArray(), inMemoryFileSystem)
            loadProtoFiles(inMemoryFileSystem, classLoader, GOOGLE_API_PROTOS, GOOGLE_API_PATH)

            createDirectory(GOOGLE_WELLKNOWN_PATH.split("/").dropLastWhile { it.isEmpty() }.toTypedArray(), inMemoryFileSystem)
            loadProtoFiles(inMemoryFileSystem, classLoader, GOOGLE_WELLKNOWN_PROTOS, GOOGLE_WELLKNOWN_PATH)

            createDirectory(WIRE_PATH.split("/").dropLastWhile { it.isEmpty() }.toTypedArray(), inMemoryFileSystem)
            loadProtoFiles(inMemoryFileSystem, classLoader, WIRE_PROTOS, WIRE_PATH)

            createDirectory(METADATA_PATH.split("/").dropLastWhile { it.isEmpty() }.toTypedArray(), inMemoryFileSystem)
            loadProtoFiles(
                inMemoryFileSystem,
                classLoader,
                Collections.singleton(METADATA_PROTO),
                METADATA_PATH,
            )

            createDirectory(DECIMAL_PATH.split("/").dropLastWhile { it.isEmpty() }.toTypedArray(), inMemoryFileSystem)
            loadProtoFiles(
                inMemoryFileSystem,
                classLoader,
                Collections.singleton(DECIMAL_PROTO),
                DECIMAL_PATH,
            )

            return inMemoryFileSystem
        }

        @Throws(IOException::class)
        private fun loadProtoFiles(
            inMemoryFileSystem: FakeFileSystem,
            classLoader: ClassLoader,
            protos: Set<String>,
            protoPath: String,
        ) {
            for (proto in protos) {
                // Loads the proto file resource files.
                val inputStream =
                    classLoader.getResourceAsStream(protoPath + proto)
                        ?: throw IOException("Proto file not found: $protoPath$proto")
                val fileContents = CharStreams.toString(InputStreamReader(inputStream, Charsets.UTF_8))
                val dir = "/".toPath().resolve(protoPath)
                inMemoryFileSystem.createDirectories(dir)
                val bytes = fileContents.toByteArray()
                val path = dir.resolve(proto)
                writeToFakeFileSystem(inMemoryFileSystem, bytes, path)
            }
        }

        @Throws(IOException::class)
        private fun writeToFakeFileSystem(
            inMemoryFileSystem: FakeFileSystem,
            bytes: ByteArray,
            path: Path,
        ) {
            val buffer = Buffer()
            buffer.write(bytes)
            val sink = inMemoryFileSystem.sink(path)
            sink.write(buffer, bytes.size.toLong())
            sink.flush()
            sink.close()
        }

        @Throws(IOException::class)
        private fun createDirectory(
            dirs: Array<String>,
            fileSystem: FakeFileSystem,
        ): Path {
            var path = "/".toPath()
            for (dir in dirs) {
                path = path.resolve(dir)
            }
            fileSystem.createDirectories(path)
            return path
        }

        /**
         * Creates a schema loader using a in-memory file system. This is required for square wire schema parser and linker
         * to load the types correctly. See https://github.com/square/wire/issues/2024#
         * As of now this only supports reading one .proto file but can be extended to support reading multiple files.
         *
         * @param packageName Package name for the .proto if present
         * @param fileName Name of the .proto file.
         * @param schemaDefinition Schema Definition to parse.
         * @return Schema - parsed and properly linked Schema.
         */
        @JvmStatic
        @Throws(IOException::class)
        fun loadSchema(
            packageName: Optional<String>,
            fileName: String,
            schemaDefinition: String,
        ): ProtobufSchemaLoaderContext {
            val inMemoryFileSystem = getFileSystem()

            var dirs: Array<String> = emptyArray()
            if (packageName.isPresent) {
                dirs = packageName.get().split("\\.".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            }
            val protoFileName = if (fileName.endsWith(".proto")) fileName else "$fileName.proto"
            try {
                val dirPath = createDirectory(dirs, inMemoryFileSystem)
                val path = dirPath.resolve(protoFileName)
                writeToFakeFileSystem(inMemoryFileSystem, schemaDefinition.toByteArray(), path)

                val schemaLoader = SchemaLoader(inMemoryFileSystem)
                schemaLoader.initRoots(
                    Lists.newArrayList(Location.get("/")),
                    Lists.newArrayList(Location.get("/")),
                )

                val schema = schemaLoader.loadSchema()
                val protoFile =
                    schema.protoFile(path.toString().replaceFirst("/", ""))
                        ?: throw RuntimeException("Error loading Protobuf File: $protoFileName")

                return ProtobufSchemaLoaderContext(schema, protoFile)
            } catch (e: Exception) {
                throw e
            }
        }
    }
}
