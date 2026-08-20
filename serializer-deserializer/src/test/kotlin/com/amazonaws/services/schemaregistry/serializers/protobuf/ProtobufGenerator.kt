package com.amazonaws.services.schemaregistry.serializers.protobuf

import Foo.Contact
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.Basic
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.ComplexNestingSyntax2
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.alltypes.AllTypesSyntax2
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.BasicSyntax2
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.ProtodevelaslProtoProtoProtodevelBar3
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.snake_case.SnakeCaseFile
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.ComplexNestingSyntax3
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.alltypes.AllTypes
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.alltypes.AnEnum
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.alltypes.AnotherTopLevelMessage
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Basicsyntax3
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.ConflictingNameOuterClass
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Foo1
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.HyphenAtedProtoFile
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.NestedConflictingClassNameOuterClass
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Unicode
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.multiplefiles.A
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.snake_case.AnotherSnakeCaseProtoFile
import com.amazonaws.services.schemaregistry.utils.apicurio.FileDescriptorUtils
import com.amazonaws.services.schemaregistry.utils.apicurio.syntax2.WellKnownTypesTestSyntax2
import com.amazonaws.services.schemaregistry.utils.apicurio.syntax3.WellKnownTypesTestSyntax3
import com.google.protobuf.Api
import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors
import com.google.protobuf.Duration
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Empty
import com.google.protobuf.EnumValue
import com.google.protobuf.Int32Value
import com.google.protobuf.ListValue
import com.google.protobuf.Message
import com.google.protobuf.Mixin
import com.google.protobuf.StringValue
import com.google.protobuf.Struct
import com.google.protobuf.Timestamp
import com.google.protobuf.UInt64Value
import com.google.protobuf.Value
import com.google.type.CalendarPeriod
import com.google.type.Color
import com.google.type.Date
import com.google.type.Fraction
import com.google.type.Money
import com.google.type.Month
import com.google.type.PhoneNumber
import com.google.type.PostalAddress
import java.util.Optional
import com.google.protobuf.Enum as ProtoEnum
import com.google.protobuf.Method as ProtoMethod

/**
 * Generates Protobuf objects to be used during testing
 */
object ProtobufGenerator {
    private const val NAME = "Foo"

    @JvmField
    val BASIC_REFERENCING_MESSAGE: Basic.Customer = Basic.Customer.newBuilder().setName(NAME).build()

    @JvmField
    val BASIC_REFERENCING_DYNAMIC_MESSAGE: DynamicMessage =
        DynamicMessage
            .newBuilder(Basic.Address.getDescriptor())
            .setField(Basic.Address.getDescriptor().findFieldByName("street"), NAME)
            .build()

    @JvmField
    val BASIC_SYNTAX2_MESSAGE: BasicSyntax2.Phone = BasicSyntax2.Phone.newBuilder().setModel(NAME).build()

    @JvmField
    val BASIC_SYNTAX3_MESSAGE: Basicsyntax3.Phone = Basicsyntax3.Phone.newBuilder().setModel(NAME).build()

    @JvmField
    val NESTING_MESSAGE_PROTO3: ComplexNestingSyntax3.A.B.C.X.D.F.M =
        ComplexNestingSyntax3.A.B.C.X.D.F.M
            .newBuilder()
            .setChoice(ComplexNestingSyntax3.A.B.C.X.D.F.M.K.L)
            .build()

    @JvmField
    val NESTING_MESSAGE_PROTO2: ComplexNestingSyntax2.O.A =
        ComplexNestingSyntax2.O.A
            .newBuilder()
            .addB("12312")
            .build()

    @JvmField
    val NESTING_MESSAGE_PROTO3_MULTIPLE_FILES: A.B.C.X.D.F.M =
        A.B.C.X.D.F.M
            .newBuilder()
            .setChoice(A.B.C.X.D.F.M.K.L)
            .build()

    @JvmField
    val JAVA_OUTER_CLASS_WITH_MULTIPLE_FILES_MESSAGE:
        com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.multiplefiles.Phone =
        com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.multiplefiles.Phone
            .newBuilder()
            .build()

    @JvmField
    val JAVA_OUTER_CLASS_MESSAGE: Contact.Phone = Contact.Phone.newBuilder().build()

    @JvmField
    val SNAKE_CASE_MESSAGE: SnakeCaseFile.snake_case_message =
        SnakeCaseFile.snake_case_message
            .newBuilder()
            .build()

    @JvmField
    val ANOTHER_SNAKE_CASE_MESSAGE: AnotherSnakeCaseProtoFile.another_SnakeCase_ =
        AnotherSnakeCaseProtoFile.another_SnakeCase_
            .newBuilder()
            .build()

    @JvmField
    val DOLLAR_SYNTAX_3_MESSAGE: Foo1.Dollar = Foo1.Dollar.newBuilder().build()

    @JvmField
    val HYPHEN_ATED_PROTO_FILE_MESSAGE: HyphenAtedProtoFile.hyphenated =
        HyphenAtedProtoFile.hyphenated
            .newBuilder()
            .build()

    @JvmField
    val DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE: ProtodevelaslProtoProtoProtodevelBar3.bar =
        ProtodevelaslProtoProtoProtodevelBar3.bar
            .newBuilder()
            .build()

    @JvmField
    val UNICODE_MESSAGE: Unicode.uni = Unicode.uni.newBuilder().build()

    @JvmField
    val CONFLICTING_NAME_MESSAGE: ConflictingNameOuterClass.ConflictingName =
        ConflictingNameOuterClass.ConflictingName
            .newBuilder()
            .build()

    @JvmField
    val NESTED_CONFLICTING_NAME_MESSAGE: NestedConflictingClassNameOuterClass.Parent.NestedConflictingClassName =
        NestedConflictingClassNameOuterClass.Parent.NestedConflictingClassName
            .newBuilder()
            .build()

    @JvmField
    val ALL_TYPES_MESSAGE_SYNTAX3: AllTypes =
        AllTypes
            .newBuilder()
            .setStringType("0asd29340932")
            .setByteType(ByteString.copyFrom(UNICODE_MESSAGE.toByteArray()))
            .setOneOfInt(93)
            .setOneOfMoney(Money.newBuilder().setCurrencyCode("INR").setUnits(4L).setNanos(2390).build())
            .addAllRepeatedString(listOf("asd", "fgf"))
            .addAllRepeatedPackedInts(listOf("1", "90", "34"))
            .setAnotherOneOfMoney(Money.newBuilder().setCurrencyCode("INR").setUnits(4L).setNanos(2390).build())
            .setOptionalSfixed32(1231)
            .setOptionalSfixed64(3092L)
            .setAnEnum2(AnEnum.ALPHA)
            .setUint64Type(1922L)
            .setInt32Type(91)
            .setSint32Type(-910)
            .setSint64Type(-9122)
            .setFixed32Type(19023)
            .setFixed64Type(123)
            .setNestedMessage1(AllTypes.NestedMessage1.newBuilder().setDoubleType(123123.1232).build())
            .putAComplexMap(
                90,
                AnotherTopLevelMessage.NestedMessage2
                    .newBuilder()
                    .addAllATimestamp(
                        listOf(
                            Timestamp.newBuilder().setSeconds(123).setNanos(1).build(),
                            Timestamp.newBuilder().setNanos(0).build(),
                        ),
                    ).build(),
            ).setAnEnum1(AnEnum.BETA)
            .putAComplexMap(
                81,
                AnotherTopLevelMessage.NestedMessage2
                    .newBuilder()
                    .addATimestamp(Timestamp.newBuilder().build())
                    .build(),
            ).build()

    @JvmField
    val ALL_TYPES_MESSAGE_SYNTAX2: AllTypesSyntax2.AllTypes =
        AllTypesSyntax2.AllTypes
            .newBuilder()
            .setStringType("0asd29340932")
            .setByteType(ByteString.copyFrom(UNICODE_MESSAGE.toByteArray()))
            .setOneOfInt(93)
            .setOneOfMoney(Money.newBuilder().setCurrencyCode("INR").setUnits(4L).setNanos(2390).build())
            .addAllRepeatedString(listOf("asd", "fgf"))
            .addAllRepeatedPackedInts(listOf("1", "90", "34"))
            .setAnotherOneOfMoney(Money.newBuilder().setCurrencyCode("INR").setUnits(4L).setNanos(2390).build())
            .setOptionalSfixed32(1231)
            .setOptionalSfixed64(3092L)
            .setAnEnum2(AllTypesSyntax2.AnEnum.BETA)
            .setUint64Type(1922L)
            .setInt32Type(91)
            .setSint32Type(-910)
            .setSint64Type(-9122)
            .setFixed32Type(19023)
            .setFixed64Type(123)
            .setNestedMessage1(
                AllTypesSyntax2.AllTypes.NestedMessage1
                    .newBuilder()
                    .setDoubleType(123123.1232)
                    .build(),
            ).putAComplexMap(
                90,
                AllTypesSyntax2.AnotherTopLevelMessage.NestedMessage2
                    .newBuilder()
                    .addAllATimestamp(
                        listOf(
                            Timestamp.newBuilder().setSeconds(123).setNanos(1).build(),
                            Timestamp.newBuilder().setNanos(0).build(),
                        ),
                    ).build(),
            ).build()

    @JvmField
    val WELL_KNOWN_TYPES_SYNTAX_2: WellKnownTypesTestSyntax2.WellKnownTypesSyntax3 =
        WellKnownTypesTestSyntax2.WellKnownTypesSyntax3
            .newBuilder()
            .setA(101)
            .setFloating(0f)
            .setF1(Timestamp.newBuilder().setSeconds(123).setNanos(1).build())
            .setF2(StringValue.newBuilder().setValue("stringValue").build())
            .setF4(Empty.newBuilder().build())
            .setF5(Duration.newBuilder().setNanos(5).setSeconds(10).build())
            .setF22(ListValue.newBuilder().addValues(Value.newBuilder().setNumberValue(2.2).build()).build())
            .setF27(Int32Value.newBuilder().setValue(27).build())
            .setF33(Struct.newBuilder().build())
            .setF35(UInt64Value.newBuilder().setValue(64).build())
            .setF37(Api.newBuilder().setName("newapi").build())
            .setF42(ProtoEnum.newBuilder().addEnumvalue(EnumValue.newBuilder().setName("enumValue").build()).build())
            .setF47(ProtoMethod.newBuilder().setName("method").setRequestTypeUrl("sampleUrl").build())
            .setF48(Mixin.newBuilder().setName("mixin").build())
            .setF9(CalendarPeriod.DAY)
            .setF10(Color.newBuilder().setRed(100f).setGreen(100f).setBlue(100f).build())
            .setF7(Date.newBuilder().setDay(1).setMonth(4).setYear(2022).build())
            .setF13(Fraction.newBuilder().setDenominator(100).setNumerator(9).build())
            .setF6(Money.newBuilder().setUnits(10).build())
            .setF14(Month.APRIL)
            .setF16(PostalAddress.newBuilder().setPostalCode("98121").build())
            .setF15(PhoneNumber.newBuilder().setE164Number("206").build())
            .build()

    @JvmField
    val WELL_KNOWN_TYPES_SYNTAX_3: WellKnownTypesTestSyntax3.WellKnownTypesSyntax3 =
        WellKnownTypesTestSyntax3.WellKnownTypesSyntax3
            .newBuilder()
            .setA(101)
            .setFloating(0f)
            .setF1(Timestamp.newBuilder().setSeconds(123).setNanos(1).build())
            .setF2(StringValue.newBuilder().setValue("stringValue").build())
            .setF4(Empty.newBuilder().build())
            .setF5(Duration.newBuilder().setNanos(5).setSeconds(10).build())
            .setF22(ListValue.newBuilder().addValues(Value.newBuilder().setNumberValue(2.2).build()).build())
            .setF27(Int32Value.newBuilder().setValue(27).build())
            .setF33(Struct.newBuilder().build())
            .setF35(UInt64Value.newBuilder().setValue(64).build())
            .setF37(Api.newBuilder().setName("newapi").build())
            .setF42(ProtoEnum.newBuilder().addEnumvalue(EnumValue.newBuilder().setName("enumValue").build()).build())
            .setF47(ProtoMethod.newBuilder().setName("method").setRequestTypeUrl("sampleUrl").build())
            .setF48(Mixin.newBuilder().setName("mixin").build())
            .setF9(CalendarPeriod.DAY)
            .setF10(Color.newBuilder().setRed(100f).setGreen(100f).setBlue(100f).build())
            .setF7(Date.newBuilder().setDay(1).setMonth(4).setYear(2022).build())
            .setF13(Fraction.newBuilder().setDenominator(100).setNumerator(9).build())
            .setF6(Money.newBuilder().setUnits(10).build())
            .setF14(Month.APRIL)
            .setF16(PostalAddress.newBuilder().setPostalCode("98121").build())
            .setF15(PhoneNumber.newBuilder().setE164Number("206").build())
            .build()

    @JvmStatic
    fun getAllPOJOMessages(): List<Message> = listOf(
        BASIC_REFERENCING_MESSAGE,
        BASIC_SYNTAX2_MESSAGE,
        BASIC_SYNTAX3_MESSAGE,
        NESTING_MESSAGE_PROTO2,
        NESTING_MESSAGE_PROTO3,
        NESTING_MESSAGE_PROTO3_MULTIPLE_FILES,
        JAVA_OUTER_CLASS_WITH_MULTIPLE_FILES_MESSAGE,
        JAVA_OUTER_CLASS_MESSAGE,
        UNICODE_MESSAGE,
        NESTED_CONFLICTING_NAME_MESSAGE,
        ALL_TYPES_MESSAGE_SYNTAX3,
        ALL_TYPES_MESSAGE_SYNTAX2,
        WELL_KNOWN_TYPES_SYNTAX_2,
        WELL_KNOWN_TYPES_SYNTAX_3,
    )

    @JvmStatic
    fun getAllDynamicMessages(): List<DynamicMessage> = listOf(
        BASIC_REFERENCING_DYNAMIC_MESSAGE,
        createDynamicProtobufRecord(),
        createDynamicNRecord(),
        createDynamicMessageFromPOJO(ALL_TYPES_MESSAGE_SYNTAX2),
        createDynamicMessageFromPOJO(ALL_TYPES_MESSAGE_SYNTAX3),
        createDynamicMessageFromPOJO(WELL_KNOWN_TYPES_SYNTAX_2),
        createDynamicMessageFromPOJO(WELL_KNOWN_TYPES_SYNTAX_3),
        // Add all types,
    )

    private fun createDynamicMessageFromPOJO(pojo: Message): DynamicMessage {
        val pojoBytes = pojo.toByteArray()
        return DynamicMessage
            .newBuilder(pojo.descriptorForType)
            .mergeFrom(pojoBytes)
            .build()
    }

    @JvmStatic
    fun createCompiledProtobufRecord(): Basic.Address = Basic.Address
        .newBuilder()
        .setStreet("410 Terry Ave. North")
        .setCity("Seattle")
        .setZip(98109)
        .build()

    @JvmStatic
    fun createDynamicProtobufRecord(): DynamicMessage {
        val fieldDescriptorList = Basic.Address.getDescriptor().fields
        return DynamicMessage
            .newBuilder(Basic.Address.getDescriptor())
            .setField(fieldDescriptorList[0], "5432 82nd St")
            .setField(fieldDescriptorList[1], 123456)
            .setField(fieldDescriptorList[2], "Seattle")
            .build()
    }

    @JvmStatic
    fun createDynamicNRecord(): DynamicMessage = DynamicMessage
        .newBuilder(ComplexNestingSyntax3.N.getDescriptor())
        .setField(ComplexNestingSyntax3.N.getDescriptor().findFieldByName("A"), 100)
        .build()

    /**
     * Creates a Message from a dynamic schema that is only compiled during runtime.
     * There are no POJOs pre-compiled for this schema.
     */
    @JvmStatic
    @Throws(Descriptors.DescriptorValidationException::class)
    fun createRuntimeCompiledRecord(): Message {
        val nonPojoExistentSchemaDefinition =
            "package foo; message NonExistentSchema { optional string a = 1; }"

        val fileDescriptor =
            FileDescriptorUtils
                .protoFileToFileDescriptor(
                    nonPojoExistentSchemaDefinition,
                    "NonExistent.proto",
                    Optional.of("foo"),
                )

        // Create a message using above fileDescriptor
        return DynamicMessage.newBuilder(fileDescriptor.findMessageTypeByName("NonExistentSchema")).build()
    }
}
