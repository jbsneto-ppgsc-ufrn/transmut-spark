package br.ufrn.dimap.forall.transmut.analyzer

import java.nio.file._

import scala.meta.internal.semanticdb.Locator
import scala.meta.internal.semanticdb.TextDocument
import scala.meta.internal.semanticdb.TextDocuments
import scala.meta.internal.semanticdb.TypeRef
import scala.meta.internal.semanticdb.SignatureMessage.SealedValue._

import br.ufrn.dimap.forall.transmut.model._
import scala.meta.internal.semanticdb.SingleType

object TypesAnalyzer {

  def getReferenceMapFromPath(pathSemanticDB: Path): Map[String, Reference] = {
    val textDocuments = getTextDocuments(pathSemanticDB)
    val referenceMap = textDocuments.documents.map(d => getReferencesMap(d)).reduce(_ ++ _)
    referenceMap
  }

  def getReferenceMapFromPath(path: String): Map[String, Reference] = getReferenceMapFromPath(Paths.get(path)) 

  def getTextDocuments(pathSemanticDB: Path): TextDocuments = {
    var textDocuments: TextDocuments = null
    Locator(pathSemanticDB)((path, documents) => {
      textDocuments = documents
    })
    textDocuments
  }

  def getTextDocuments(path: String): TextDocuments = getTextDocuments(Paths.get(path))

  def getReferencesMap(document: TextDocument): Map[String, Reference] = {

    val referencesMap = scala.collection.mutable.Map[String, Reference]()
    
    // To use in the case of references to find later (this.type)
    val symbolDisplayNameMap = scala.collection.mutable.Map[String, String]() // symbol -> displayName
    val referencesToFindLater = scala.collection.mutable.Map[String, String]() // displayName -> symbol

    for (symbol <- document.symbols) {
      val name = symbol.displayName
      val kind = symbol.kind

      // To use in the case of references to find later (this.type)
      symbolDisplayNameMap += (symbol.symbol -> name)

      val valueType = symbol.signature.asMessage.sealedValue match {
        case v: ValueSignature => v.value.tpe match {
          case t: TypeRef => typeFromTypeRef(t)
          case SingleType(prefix, symb) => {
            referencesToFindLater += (name -> symb) // such references are marked for later discovery (this.type)
            ErrorType()
          }
          case _ => ErrorType()
        }
        case m: MethodSignature => m.value.returnType match {
          case t: TypeRef => typeFromTypeRef(t)
          case SingleType(prefix, symb) => {
            referencesToFindLater += (name -> symb) // such references are marked for later discovery (this.type)
            ErrorType()
          }
          case _ => ErrorType()
        }
        case c: ClassSignature => ClassType(name)
        case t: TypeSignature  => ClassType(name)
        case o                 => ErrorType()
      }
      var reference: Reference = ErrorReference()
      if (kind.isClass || kind.isTrait || kind.isObject || kind.isClass) {
        reference = ClassReference(name)
      } else if (kind.isMethod) {
        reference = MethodReference(name, valueType)
      } else if (kind.isParameter) {
        reference = ParameterReference(name, valueType)
      } else if (kind.isType) {
        reference = TypeReference(name)
      } else if (kind.isLocal) {
        reference = ValReference(name, valueType)
      }
      referencesMap += (name -> reference)
    }

    // Update the references that have self.type as type and are captured as SingleType with a local symbol
    if (!referencesToFindLater.isEmpty) {
      for ((displayName, symbol) <- referencesToFindLater) {
        val originalSymbolDisplayName = symbolDisplayNameMap.get(symbol)
        if (originalSymbolDisplayName.isDefined) {
          val referenceToCopyType = referencesMap.get(originalSymbolDisplayName.get)
          if (referenceToCopyType.isDefined) {
            val updatedReference: Reference = referenceToCopyType.get match {
              case ClassReference(name)                => ClassReference(displayName)
              case MethodReference(name, valueType)    => MethodReference(displayName, valueType)
              case ParameterReference(name, valueType) => ParameterReference(displayName, valueType)
              case TypeReference(name)                 => TypeReference(displayName)
              case ValReference(name, valueType)       => ValReference(displayName, valueType)
              case _                                   => ErrorReference()
            }
            if (!updatedReference.isInstanceOf[ErrorReference]) {
              referencesMap.update(displayName, updatedReference)
            }
          }
        }
      }
    }

    referencesMap.toMap
  }

  def typeFromTypeRef(t: TypeRef): Type = t match {
    case TypeRef(prefix, symbol, typeArguments) if typeArguments.isEmpty && BaseTypesEnum.isBaseType(symbol)  => BaseType(BaseTypesEnum.baseTypeFromName(symbol))
    case TypeRef(prefix, symbol, typeArguments) if typeArguments.isEmpty && !BaseTypesEnum.isBaseType(symbol) => ClassType(symbol)
    case TypeRef(prefix, symbol, typeArguments) if symbol.contains("Tuple2") && typeArguments.size == 2 => {
      val keyTypeRef = typeArguments(0) match {
        case t: TypeRef => t
        case _          => null
      }
      val valueTypeRef = typeArguments(1) match {
        case t: TypeRef => t
        case _          => null
      }
      TupleType(typeFromTypeRef(keyTypeRef), typeFromTypeRef(valueTypeRef))
    }
    case TypeRef(prefix, symbol, typeArguments) if !symbol.contains("Tuple2") && typeArguments.size > 0 => {
      val listTypes = scala.collection.mutable.ListBuffer.empty[Type]
      typeArguments.foreach { ty =>
        ty match {
          case t: TypeRef => {
            val paramType = typeFromTypeRef(t)
            listTypes += paramType
          }
          case _ => {}
        }
      }
      ParameterizedType(symbol, listTypes.toList)
    }
    case _ => ErrorType()
  }

}