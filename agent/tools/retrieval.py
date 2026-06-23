#
#  Copyright 2024 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import asyncio
from functools import partial
import json
import logging
import os
import re
from abc import ABC
from agent.tools.base import ToolParamBase, ToolBase, ToolMeta
from common.constants import LLMType
from api.db.services.doc_metadata_service import DocMetadataService
from common.metadata_utils import apply_meta_data_filter
from api.db.services.knowledgebase_service import KnowledgebaseService
from api.db.services.llm_service import LLMBundle
from api.db.services.memory_service import MemoryService
from api.db.joint_services import memory_message_service
from api.db.joint_services.tenant_model_service import get_tenant_default_model_by_type, get_model_config_from_provider_instance
from common import settings
from common.connection_utils import timeout
from rag.app.tag import label_question
from rag.prompts.generator import cross_languages, kb_prompt, memory_prompt


class RetrievalParam(ToolParamBase):
    """
    Define the Retrieval component parameters.
    """

    def __init__(self):
        self.meta:ToolMeta = {
            "name": "search_my_dateset",
            "description": "Search the selected dataset for content relevant to the user's current question.",
            "parameters": {
                "query": {
                    "type": "string",
                    "description": "A concise search query derived from the user's current question. Keep the user's core entities, standards, product names, people, places, codes, or document titles. Do not replace the user's question with generic domain examples or node descriptions.",
                    "default": "",
                    "required": True
                }
            }
        }
        super().__init__()
        self.function_name = "search_my_dateset"
        self.description = "Search the selected dataset for content relevant to the user's current question."
        self.similarity_threshold = 0.2
        self.keywords_similarity_weight = 0.5
        self.top_n = 8
        self.top_k = 1024
        self.dataset_ids = []
        self.kb_ids = []  # Deprecated: keep for backward compatibility
        self.memory_ids = []
        self.kb_vars = []
        self.rerank_id = ""
        self.empty_response = ""
        self.use_kg = False
        self.cross_languages = []
        self.toc_enhance = False
        self.meta_data_filter={}

    def check(self):
        self.check_decimal_float(self.similarity_threshold, "[Retrieval] Similarity threshold")
        self.check_decimal_float(self.keywords_similarity_weight, "[Retrieval] Keyword similarity weight")
        self.check_positive_number(self.top_n, "[Retrieval] Top N")

    def get_input_form(self) -> dict[str, dict]:
        return {
            "query": {
                "name": "Query",
                "type": "line"
            }
        }

class Retrieval(ToolBase, ABC):
    component_name = "Retrieval"

    _GENERIC_QUERY_TOKENS = {
        "企业知识库",
        "项目计划",
        "部门规范",
        "流程模板",
        "培训考核",
        "造价",
        "关键任务",
        "测试资料",
    }
    _LOW_VALUE_QUERY_TOKENS = {
        "一下",
        "什么",
        "关于",
        "查询",
        "知识",
        "知识库",
        "要求",
        "相关",
        "信息",
        "哪里",
        "在哪",
        "地址",
        "网址",
        "登录",
        "入口",
    }

    @property
    def _dataset_ids(self):
        """Get dataset IDs with backward compatibility for kb_ids."""
        return self._param.dataset_ids or getattr(self._param, "kb_ids", None) or []

    def _normalize_query_text(self, query_text: str) -> str:
        query = (query_text or "").strip()
        sys_query = (self._canvas.get_sys_query() or "").strip()
        if not sys_query:
            return query

        if not query:
            return sys_query

        query_compact = re.sub(r"\s+", " ", query)
        sys_query_compact = re.sub(r"\s+", " ", sys_query)
        if query_compact == sys_query_compact:
            return query

        if self._is_unrelated_tool_query(query_compact, sys_query_compact):
            logging.warning(
                "[Retrieval] Replace unrelated tool query with sys.query. tool_query=%r sys_query=%r",
                query_compact,
                sys_query_compact,
            )
            return sys_query

        generic_token_hits = sum(1 for token in self._GENERIC_QUERY_TOKENS if token in query_compact)
        looks_like_generic_prompt = generic_token_hits >= 2
        looks_like_real_question = any(ch in query for ch in "?:？：") or len(query_compact) <= 24
        sys_query_is_specific = len(sys_query_compact) >= 4

        if looks_like_generic_prompt and sys_query_is_specific and not looks_like_real_question:
            logging.warning(
                "[Retrieval] Replace suspicious tool query with sys.query. tool_query=%r sys.query=%r",
                query_compact,
                sys_query_compact,
            )
            return sys_query

        return query

    @classmethod
    def _is_unrelated_tool_query(cls, query: str, sys_query: str) -> bool:
        query_terms = cls._meaningful_query_terms(query)
        sys_query_terms = cls._meaningful_query_terms(sys_query)
        if len(query_terms) < 2 or len(sys_query_terms) < 2:
            return False
        return not bool(query_terms & sys_query_terms)

    @classmethod
    def _meaningful_query_terms(cls, text: str) -> set[str]:
        terms = {
            token
            for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]+", (text or "").lower())
            if len(token) >= 2
        }
        for segment in re.findall(r"[\u4e00-\u9fff]+", text or ""):
            if len(segment) == 1:
                continue
            if len(segment) <= 8:
                terms.add(segment)
            terms.update(segment[index : index + 2] for index in range(len(segment) - 1))
        return {term for term in terms if term not in cls._LOW_VALUE_QUERY_TOKENS}

    async def _retrieve_kb(self, query_text: str):
        kb_ids: list[str] = []
        for id in self._dataset_ids:
            if id.find("@") < 0:
                kb_ids.append(id)
                continue
            kb_nm = self._canvas.get_variable_value(id)
            # if kb_nm is a list
            kb_nm_list = kb_nm if isinstance(kb_nm, list) else [kb_nm]
            for nm_or_id in kb_nm_list:
                e, kb = KnowledgebaseService.get_by_name(nm_or_id,
                                                         self._canvas._tenant_id)
                if not e:
                    e, kb = KnowledgebaseService.get_by_id(nm_or_id)
                    if not e:
                        raise Exception(f"Dataset({nm_or_id}) does not exist.")
                kb_ids.append(kb.id)

        filtered_kb_ids: list[str] = list(set([kb_id for kb_id in kb_ids if kb_id]))

        kbs = KnowledgebaseService.get_by_ids(filtered_kb_ids)
        if not kbs:
            raise Exception("No dataset is selected.")

        embd_nms = list(set([kb.embd_id for kb in kbs]))
        assert len(embd_nms) == 1, "Knowledge bases use different embedding models."

        embd_mdl = None
        if embd_nms:
            tenant_id = self._canvas.get_tenant_id()
            embd_model_config = get_model_config_from_provider_instance(tenant_id, LLMType.EMBEDDING, embd_nms[0])
            embd_mdl = LLMBundle(tenant_id, embd_model_config)

        rerank_mdl = None
        if self._param.rerank_id:
            rerank_model_config = get_model_config_from_provider_instance(kbs[0].tenant_id, LLMType.RERANK, self._param.rerank_id)
            rerank_mdl = LLMBundle(kbs[0].tenant_id, rerank_model_config)

        vars = self.get_input_elements_from_text(query_text)
        vars = {k: o["value"] for k, o in vars.items()}
        query = self.string_format(query_text, vars)
        query = self._normalize_query_text(query)

        doc_ids = []
        if self._param.meta_data_filter != {}:
            # Defer the (potentially expensive) metadata table load — manual
            # filters served by ES push-down never need it. The loader is
            # invoked at most once per request by ``apply_meta_data_filter``.
            def _load_metas() -> dict:
                return DocMetadataService.get_flatted_meta_by_kbs(kb_ids)

            def _resolve_manual_filter(flt: dict) -> dict:
                # Return a new dict instead of mutating `flt` in place. The
                # caller passes filters straight out of self._param.meta_data_filter,
                # so mutating them would replace the variable reference with its
                # resolved value and every subsequent invocation (e.g. inside an
                # Iteration component) would reuse that stale value.
                pat = re.compile(self.variable_ref_patt)
                s = flt.get("value", "")
                out_parts = []
                last = 0

                for m in pat.finditer(s):
                    out_parts.append(s[last:m.start()])
                    key = m.group(1)
                    v = self._canvas.get_variable_value(key)
                    if v is None:
                        rep = ""
                    elif isinstance(v, partial):
                        buf = []
                        for chunk in v():
                            buf.append(chunk)
                        rep = "".join(buf)
                    elif isinstance(v, str):
                        rep = v
                    else:
                        rep = json.dumps(v, ensure_ascii=False)

                    out_parts.append(rep)
                    last = m.end()

                out_parts.append(s[last:])
                resolved = dict(flt)
                resolved["value"] = "".join(out_parts)
                return resolved

            chat_mdl = None
            if self._param.meta_data_filter.get("method") in ["auto", "semi_auto"]:
                tenant_id = self._canvas.get_tenant_id()
                chat_model_config = get_tenant_default_model_by_type(tenant_id, LLMType.CHAT)
                chat_mdl = LLMBundle(tenant_id, chat_model_config)

            doc_ids = await apply_meta_data_filter(
                self._param.meta_data_filter,
                None,
                query,
                chat_mdl,
                doc_ids,
                _resolve_manual_filter if self._param.meta_data_filter.get("method") == "manual" else None,
                kb_ids=kb_ids,
                metas_loader=_load_metas,
            )

        if self._param.cross_languages:
            query = await cross_languages(kbs[0].tenant_id, None, query, self._param.cross_languages)

        if kbs:
            query = re.sub(r"^user[:：\s]*", "", query, flags=re.IGNORECASE)
            kbinfos = await settings.retriever.retrieval(
                query,
                embd_mdl,
                [kb.tenant_id for kb in kbs],
                filtered_kb_ids,
                1,
                self._param.top_n,
                self._param.similarity_threshold,
                1 - self._param.keywords_similarity_weight,
                top=self._param.top_k,
                doc_ids=doc_ids,
                aggs=True,
                rerank_mdl=rerank_mdl,
                rank_feature=label_question(query, kbs),
            )
            if self.check_if_canceled("Retrieval processing"):
                return

            if self._param.toc_enhance:
                tenant_id = self._canvas._tenant_id
                chat_model_config = get_tenant_default_model_by_type(tenant_id, LLMType.CHAT)
                chat_mdl = LLMBundle(tenant_id, chat_model_config)
                cks = await settings.retriever.retrieval_by_toc(query, kbinfos["chunks"], [kb.tenant_id for kb in kbs],
                                                          chat_mdl, self._param.top_n)
                if self.check_if_canceled("Retrieval processing"):
                    return
                if cks:
                    kbinfos["chunks"] = cks
            kbinfos["chunks"] = settings.retriever.retrieval_by_children(kbinfos["chunks"],
                                                                         [kb.tenant_id for kb in kbs])
            if self._param.use_kg:
                tenant_id = self._canvas.get_tenant_id()
                chat_model_config = get_tenant_default_model_by_type(tenant_id, LLMType.CHAT)
                ck = await settings.kg_retriever.retrieval(query,
                                                     [kb.tenant_id for kb in kbs],
                                                     kb_ids,
                                                     embd_mdl,
                                                     LLMBundle(tenant_id, chat_model_config))
                if self.check_if_canceled("Retrieval processing"):
                    return
                if ck["content_with_weight"]:
                    kbinfos["chunks"].insert(0, ck)
        else:
            kbinfos = {"chunks": [], "doc_aggs": []}

        if self._param.use_kg and kbs:
            chat_model_config = get_tenant_default_model_by_type(kbs[0].tenant_id, LLMType.CHAT)
            ck = await settings.kg_retriever.retrieval(query, [kb.tenant_id for kb in kbs], filtered_kb_ids, embd_mdl,
                                                 LLMBundle(kbs[0].tenant_id, chat_model_config))
            if self.check_if_canceled("Retrieval processing"):
                return
            if ck["content_with_weight"]:
                ck["content"] = ck["content_with_weight"]
                del ck["content_with_weight"]
                kbinfos["chunks"].insert(0, ck)

        for ck in kbinfos["chunks"]:
            if "vector" in ck:
                del ck["vector"]
            if "content_ltks" in ck:
                del ck["content_ltks"]

        if not kbinfos["chunks"]:
            self.set_output("formalized_content", self._param.empty_response)
            return

        # Format the chunks for JSON output (similar to how other tools do it)
        json_output = kbinfos["chunks"].copy()

        self._canvas.add_reference(kbinfos["chunks"], kbinfos["doc_aggs"])
        form_cnt = "\n".join(kb_prompt(kbinfos, 200000, True))

        # Set both formalized content and JSON output
        self.set_output("formalized_content", form_cnt)
        self.set_output("json", json_output)

        return form_cnt

    async def _retrieve_memory(self, query_text: str):
        memory_ids: list[str] = [memory_id for memory_id in self._param.memory_ids]
        user_id: str = self._param.user_id if hasattr(self._param, "user_id") else None
        memory_list = MemoryService.get_by_ids(memory_ids)
        if not memory_list:
            self.set_output("formalized_content", self._param.empty_response)
            return ""

        embd_names = list({memory.embd_id for memory in memory_list})
        assert len(embd_names) == 1, "Memory use different embedding models."

        vars = self.get_input_elements_from_text(query_text)
        vars = {k: o["value"] for k, o in vars.items()}
        query = self.string_format(query_text, vars)
        query = self._normalize_query_text(query)
        # query message
        filter_dict: dict = {"memory_id": memory_ids}
        if user_id:
            import re
            # is variable
            if re.match(r"^{.*}$", user_id):
                user_id = self._canvas.get_variable_value(user_id)
            filter_dict["user_id"] = user_id
        message_list = memory_message_service.query_message(filter_dict, {
            "query": query,
            "similarity_threshold": self._param.similarity_threshold,
            "keywords_similarity_weight": self._param.keywords_similarity_weight,
            "top_n": self._param.top_n
        })
        if not message_list:
            self.set_output("formalized_content", self._param.empty_response)
            return ""
        formated_content = "\n".join(memory_prompt(message_list, 200000))
        # set formalized_content output
        self.set_output("formalized_content", formated_content)

        return formated_content

    @timeout(int(os.environ.get("COMPONENT_EXEC_TIMEOUT", 12)))
    async def _invoke_async(self, **kwargs):
        if self.check_if_canceled("Retrieval processing"):
            return
        if not kwargs.get("query"):
            self.set_output("formalized_content", self._param.empty_response)
            return

        if hasattr(self._param, "retrieval_from") and self._param.retrieval_from == "dataset":
            return await self._retrieve_kb(kwargs["query"])
        elif hasattr(self._param, "retrieval_from") and self._param.retrieval_from == "memory":
            return await self._retrieve_memory(kwargs["query"])
        elif self._dataset_ids:
            return await self._retrieve_kb(kwargs["query"])
        elif hasattr(self._param, "memory_ids") and self._param.memory_ids:
            return await self._retrieve_memory(kwargs["query"])
        else:
            self.set_output("formalized_content", self._param.empty_response)
            return

    @timeout(int(os.environ.get("COMPONENT_EXEC_TIMEOUT", 12)))
    def _invoke(self, **kwargs):
        return asyncio.run(self._invoke_async(**kwargs))

    def thoughts(self) -> str:
        return """
Keywords: {}
Looking for the most relevant articles.
        """.format(self.get_input().get("query", "-_-!"))
