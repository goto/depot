package com.gotocompany.depot.kafka.mapping;

import com.google.protobuf.Descriptors;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.types.StructTypeReference;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class ProtoMappingParser {

    private final Map<String, CelRuntime.Program> compiledPrograms;
    private final String sourceBindingName;

    public ProtoMappingParser(String mappingJson, Descriptors.Descriptor sourceDescriptor) {
        this.sourceBindingName = sourceDescriptor.getFullName();
        this.compiledPrograms = compile(mappingJson, sourceDescriptor);
    }

    private Map<String, CelRuntime.Program> compile(String mappingJson, Descriptors.Descriptor sourceDescriptor) {
        JSONObject mapping = new JSONObject(mappingJson);
        Map<String, CelRuntime.Program> programs = new HashMap<>();

        CelCompiler compiler = CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
                .addMessageTypes(sourceDescriptor)
                .addVar(sourceBindingName, StructTypeReference.create(sourceDescriptor.getFullName()))
                .build();

        CelRuntime runtime = CelRuntimeFactory.standardCelRuntimeBuilder()
                .addMessageTypes(sourceDescriptor)
                .build();

        for (String outputField : mapping.keySet()) {
            String celExpression = mapping.getString(outputField);
            try {
                CelAbstractSyntaxTree ast = compiler.compile(celExpression).getAst();
                CelRuntime.Program program = runtime.createProgram(ast);
                programs.put(outputField, program);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Failed to compile CEL expression for field '" + outputField + "': " + celExpression, e);
            }
        }
        return programs;
    }

    public Map<String, CelRuntime.Program> getCompiledPrograms() {
        return compiledPrograms;
    }

    public String getSourceBindingName() {
        return sourceBindingName;
    }
}
