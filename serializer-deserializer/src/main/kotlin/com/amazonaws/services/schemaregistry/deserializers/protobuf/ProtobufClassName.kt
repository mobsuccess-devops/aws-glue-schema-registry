package com.amazonaws.services.schemaregistry.deserializers.protobuf

import com.google.protobuf.Descriptors
import org.apache.commons.lang3.StringUtils

object ProtobufClassName {
    /**
     * Derives the Protobuf generated Java class name using the Descriptor. This logic is
     * reverse-engineered from how the Protobuf compiler generates the Java classes.
     *
     * For the schema below, the class name is `foo.bar.com.Sample$abc$def`:
     * ```
     * //File name: Sample.proto
     * package foo.bar.com;
     * message abc {
     *     message def {}
     * }
     * ```
     */
    @JvmStatic
    fun from(messageDescriptor: Descriptors.Descriptor): String {
        val fileDescriptor = messageDescriptor.file
        val fileOptions = fileDescriptor.options

        // Use 'java_package' if defined explicitly.
        val packageString = if (fileOptions.hasJavaPackage()) fileOptions.javaPackage else fileDescriptor.`package`

        val outerClassName = getOuterClassName(messageDescriptor)
        val innerClassName = getInnerClassName(messageDescriptor)

        val classDelimiter = if (outerClassName.isNotEmpty()) "$" else ""
        val relativeClassName = outerClassName + classDelimiter + innerClassName

        return "$packageString.$relativeClassName"
    }

    private fun getOuterClassName(descriptor: Descriptors.Descriptor): String {
        val fileDescriptor = descriptor.file
        val fileOptions = fileDescriptor.options
        // If the Protobuf is compiled into multiple files, the outer class name is not needed.
        if (fileOptions.javaMultipleFiles) {
            return ""
        }

        if (fileOptions.hasJavaOuterClassname()) {
            return fileOptions.javaOuterClassname
        }

        val className = normalize(fileDescriptor.fullName)

        // If className is same as descriptor name, `OuterClass` is suffixed by Protobuf compiler.
        if (className == descriptor.name) {
            return className + "OuterClass"
        }

        return className
    }

    private fun stripExtension(name: String): String {
        val extensionIndex = name.lastIndexOf(".proto")
        return if (extensionIndex == -1) name else name.substring(0, extensionIndex)
    }

    /**
     * Normalizes the fileDescriptorName to a class name. The Protobuf compiler replaces special
     * characters and camel cases it to generate the class name.
     */
    @JvmStatic
    fun normalize(fileDescriptorName: String): String {
        val strippedFileDescriptorName = stripExtension(fileDescriptorName)
        val parts = strippedFileDescriptorName.split(Regex("[^a-zA-Z0-9]"))
        val camelCaseName = StringBuilder()
        for (part in parts) {
            if (StringUtils.isBlank(part)) {
                continue
            }
            if (part.length == 1) {
                camelCaseName.append(part.uppercase())
                continue
            }
            camelCaseName.append(part.substring(0, 1).uppercase() + part.substring(1))
        }
        if (strippedFileDescriptorName.lastIndexOf('#') == strippedFileDescriptorName.length - 1) {
            camelCaseName.append("_")
        }
        return camelCaseName.toString()
    }

    /**
     * Constructs the inner class name by traversing the container classes of the message
     * descriptor: a descriptor 'F' nested as 'A.B.C.D.E.F' yields 'A$B$C$D$E$F'.
     */
    private fun getInnerClassName(messageDescriptor: Descriptors.Descriptor): String {
        val inner = StringBuilder()
        inner.insert(0, messageDescriptor.name)

        var descriptor = messageDescriptor.containingType
        while (descriptor != null) {
            inner.insert(0, descriptor.name + "$")
            descriptor = descriptor.containingType
        }

        return inner.toString()
    }
}
