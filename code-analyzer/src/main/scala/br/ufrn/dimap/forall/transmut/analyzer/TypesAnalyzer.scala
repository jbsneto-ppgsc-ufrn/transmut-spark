package br.ufrn.dimap.forall.transmut.analyzer

import java.nio.file._

import scala.meta.internal.semanticdb.Locator
import scala.meta.internal.semanticdb.TextDocument
import scala.meta.internal.semanticdb.TextDocuments
import scala.meta.internal.semanticdb.TypeRef
import scala.meta.internal.semanticdb.SignatureMessage.SealedValue._

import br.ufrn.dimap.forall.transmut.model._

object TypesAnalyzer {
  
  def getReferenceMapFromPath(pathSemanticDB: Path) : Map[String, Reference] = {
    val textDocuments = getTextDocuments(pathSemanticDB)
    val referenceMap = textDocuments.documents.map(d => getReferencesMap(d)).reduce(_ ++ _)
    referenceMap
  }
  
  def getReferenceMapFromPath(path: String) : Map[String, Reference] = getReferenceMapFromPath(Paths.get(path))

  def getTextDocuments(pathSemanticDB: Path): TextDocuments = {
    var textDocuments: TextDocuments = null
    Locator(pathSemanticDB)((path, documents) => {
      textDocuments = documents
    })
    textDocuments
  }

  def getTextDocuments(path: String): TextDocuments = getTextDocuments(Paths.get(path))
  
  def getReferencesMap(document: TextDocument) : Map[String, Reference] = {
    
    val referencesMap = scala.collection.mutable.Map[String, Reference]()
    
    for (symbol <- document.symbols) {
        val name = symbol.displayName
        val kind = symbol.kind
        val valueType = symbol.signature.asMessage.sealedValue match {
          case v: ValueSignature => v.value.tpe match {
              case t: TypeRef => typeFromTypeRef(t)
              case _ => ErrorType()
            }
          case m: MethodSignature => m.value.returnType match {
              case t: TypeRef => typeFromTypeRef(t)
              case _ => ErrorType()
            }
          case c : ClassSignature => ClassType(name)
          case t : TypeSignature => ClassType(name)
          case _ => ErrorType()
        }
        var reference: Reference = ErrorReference()
        if(kind.isClass || kind.isTrait || kind.isObject || kind.isClass) {
          reference = ClassReference(name)
        } else if(kind.isMethod) {
          reference = MethodReference(name, valueType)
        } else if(kind.isParameter) {
          reference = ParameterReference(name, valueType)
        } else if(kind.isType) {
          reference = TypeReference(name)
        } else if(kind.isLocal) {
          reference = ValReference(name, valueType)
        }
        referencesMap += (name -> reference)
      }
    
    referencesMap.toMap
  }

  def typeFromTypeRef(t: TypeRef): Type = t match {
    case TypeRef(prefix, symbol, typeArguments) if typeArguments.isEmpty && BaseTypesEnum.isBaseType(symbol)  => BaseType(BaseTypesEnum.baseTypeFromName(symbol))
    case TypeRef(prefix, symbol, typeArguments) if typeArguments.isEmpty && !BaseTypesEnum.isBaseType(symbol) => ClassType(symbol)
    case TypeRef(prefix, symbol, typeArguments) if symbol.contains("Tuple2") && typeArguments.size == 2 => {
      val keyTypeRef = typeArguments(0) match {
        case t: TypeRef => t
        case _ => null
      }
      val valueTypeRef = typeArguments(1) match {
        case t: TypeRef => t
        case _ => null
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